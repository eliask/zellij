use isahc::http::{header::LOCATION, StatusCode};
use isahc::prelude::*;
use isahc::{config::RedirectPolicy, AsyncBody, HttpClient, Request, Response};
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::{io::AsyncWriteExt as _, sync::Mutex};
use tokio_stream::StreamExt as _;
use tokio_util::compat::FuturesAsyncReadCompatExt as _;
use tokio_util::io::ReaderStream;
use url::Url;

const STREAM_BUFFER_SIZE_BYTES: usize = 65535;
const MAX_DOWNLOAD_BYTES: usize = 256 * 1024 * 1024;
/// One deadline covers the whole exchange: connecting, every redirect hop,
/// response headers and the body. A server that accepts the connection but
/// never answers is bounded by the same limit as a slow body.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(300);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Redirects are followed manually so that every hop is re-validated
/// against the https-only policy; this bounds how many hops we accept.
const MAX_REDIRECTS: usize = 10;

#[derive(Error, Debug)]
pub enum DownloaderError {
    #[error("RequestError: {0}")]
    Request(#[from] isahc::Error),
    #[error("HttpError: {0}")]
    HttpError(#[from] isahc::http::Error),
    #[error("IoError: {0}")]
    Io(#[source] std::io::Error),
    #[error("StdIoError: {0}")]
    StdIoError(#[from] std::io::Error),
    #[error("File name cannot be found in URL: {0}")]
    NotFoundFileName(String),
    #[error("Failed to parse URL body: {0}")]
    InvalidUrlBody(String),
    #[error("HTTP client could not be initialized")]
    ClientUnavailable,
    #[error("Unexpected HTTP status: {0}")]
    UnexpectedStatus(u16),
    #[error("Download exceeds the maximum allowed size")]
    TooLarge,
    #[error("Download exceeded the deadline")]
    TimedOut,
    #[error("Refusing insecure download URL: {0}")]
    InsecureUrl(String),
    #[error("Invalid download URL: {0}")]
    InvalidUrl(String),
    #[error("Server returned a redirect without a usable Location")]
    InvalidRedirect,
    #[error("Too many redirects")]
    TooManyRedirects,
}

#[derive(Debug, Clone)]
pub struct Downloader {
    client: Option<HttpClient>,
    location: PathBuf,
    // the whole thing is an Arc/Mutex so that Downloader is thread safe, and the individual values of
    // the HashMap are Arc/Mutexes (Mutexi?) to represent that individual downloads should not
    // happen concurrently
    download_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    deadline: Duration,
}

/// Automatic redirect following is disabled: the client would otherwise
/// follow an https URL to plain http (or through a plain-http hop whose
/// Location an on-path attacker controls) without re-checking the policy.
fn build_client() -> Option<HttpClient> {
    HttpClient::builder()
        .redirect_policy(RedirectPolicy::None)
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .ok()
}

impl Default for Downloader {
    fn default() -> Self {
        Self {
            client: build_client(),
            location: PathBuf::from(""),
            download_locks: Default::default(),
            deadline: DOWNLOAD_TIMEOUT,
        }
    }
}

impl Downloader {
    pub fn new(location: PathBuf) -> Self {
        Self {
            client: build_client(),
            location,
            download_locks: Default::default(),
            deadline: DOWNLOAD_TIMEOUT,
        }
    }

    pub async fn download(
        &self,
        url: &str,
        file_name: Option<&str>,
    ) -> Result<(), DownloaderError> {
        let url = ensure_allowed_download_url(url)?;
        let Some(client) = &self.client else {
            log::error!("No Http client found, cannot perform requests - this is likely a misconfiguration of isahc::HttpClient");
            return Err(DownloaderError::ClientUnavailable);
        };
        let file_name = match file_name {
            Some(name) => name.to_string(),
            None => self.parse_name(url.as_str())?,
        };

        // we do this to make sure only one download of a specific url is happening at a time
        // otherwise the downloads corrupt each other (and we waste lots of system resources)
        let download_lock = self.acquire_download_lock(&file_name).await;
        // it's important that _lock remains in scope, otherwise it gets dropped and the lock is
        // released before the download is complete
        let _lock = download_lock.lock().await;

        let file_path = self.location.join(file_name.as_str());
        if file_path.exists() {
            log::debug!("File already exists: {:?}", file_path);
            return Ok(());
        }
        let file_part_path = self.location.join(format!("{}.part", file_name));
        match tokio::time::timeout(
            self.deadline,
            Self::fetch_to_part_file(client, url, &file_part_path),
        )
        .await
        {
            Ok(Ok(())) => {},
            Ok(Err(error)) => {
                if matches!(error, DownloaderError::TooLarge) {
                    let _ = tokio::fs::remove_file(&file_part_path).await;
                }
                return Err(error);
            },
            // A timed-out partial file is kept so that a later attempt can
            // resume it; its prefix is validated against Content-Range then.
            Err(_) => return Err(DownloaderError::TimedOut),
        }

        log::debug!("Download complete: {:?}", file_part_path);

        tokio::fs::rename(file_part_path, file_path)
            .await
            .map_err(|e| DownloaderError::Io(e))?;

        Ok(())
    }

    async fn fetch_to_part_file(
        client: &HttpClient,
        url: Url,
        file_part_path: &PathBuf,
    ) -> Result<(), DownloaderError> {
        let (mut target, mut file_part_size) = {
            if file_part_path.exists() {
                let file_part = tokio::fs::OpenOptions::new()
                    .append(true)
                    .write(true)
                    .open(file_part_path)
                    .await
                    .map_err(|e| DownloaderError::Io(e))?;

                let file_part_size = file_part
                    .metadata()
                    .await
                    .map_err(|e| DownloaderError::Io(e))?
                    .len();

                log::debug!("Resuming download from {} bytes", file_part_size);

                (file_part, file_part_size)
            } else {
                let file_part = tokio::fs::File::create(file_part_path)
                    .await
                    .map_err(|e| DownloaderError::Io(e))?;

                (file_part, 0)
            }
        };
        let mut res =
            send_following_redirects(client, &url, (file_part_size > 0).then_some(file_part_size))
                .await?;
        if file_part_size > 0 && res.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            // The partial file may be complete, stale or longer than the
            // current artifact. None of that can be verified, so restart
            // from an empty file rather than failing every later attempt.
            log::debug!("Resume range not satisfiable; restarting download");
            target = tokio::fs::File::create(file_part_path)
                .await
                .map_err(DownloaderError::Io)?;
            file_part_size = 0;
            res = send_following_redirects(client, &url, None).await?;
        } else if file_part_size > 0
            && res.status() == StatusCode::PARTIAL_CONTENT
            && content_range_start(&res) != Some(file_part_size)
        {
            // The server answered with a different range than requested, so
            // its bytes do not continue the partial file.
            target = tokio::fs::File::create(file_part_path)
                .await
                .map_err(DownloaderError::Io)?;
            file_part_size = 0;
            res = send_following_redirects(client, &url, None).await?;
        }
        ensure_artifact_status(res.status(), file_part_size > 0)?;
        if file_part_size > 0 && res.status() != StatusCode::PARTIAL_CONTENT {
            // The server ignored the range and is sending the whole artifact:
            // appending it to the partial file would cache a corrupt plugin.
            target = tokio::fs::File::create(file_part_path)
                .await
                .map_err(DownloaderError::Io)?;
            file_part_size = 0;
        }
        let body = res.body_mut();
        let mut stream = ReaderStream::with_capacity(body.compat(), STREAM_BUFFER_SIZE_BYTES);
        let mut downloaded_bytes =
            usize::try_from(file_part_size).map_err(|_| DownloaderError::TooLarge)?;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(DownloaderError::Io)?;
            downloaded_bytes = checked_download_total(downloaded_bytes, chunk.len())?;
            target
                .write_all(&chunk)
                .await
                .map_err(DownloaderError::Io)?;
        }
        // Writes to a tokio file complete in the background; flush so a
        // failed final write is reported instead of renaming a short file
        // into the cache.
        target.flush().await.map_err(DownloaderError::Io)?;
        Ok(())
    }
    pub async fn download_without_cache(url: &str) -> Result<String, DownloaderError> {
        Self::download_without_cache_within(url, DOWNLOAD_TIMEOUT).await
    }

    async fn download_without_cache_within(
        url: &str,
        deadline: Duration,
    ) -> Result<String, DownloaderError> {
        let url = ensure_allowed_download_url(url)?;
        let client = build_client().ok_or(DownloaderError::ClientUnavailable)?;

        let downloaded_bytes = tokio::time::timeout(deadline, async {
            let mut res = send_following_redirects(&client, &url, None).await?;
            ensure_artifact_status(res.status(), false)?;
            let mut downloaded_bytes: Vec<u8> = vec![];
            let body = res.body_mut();
            let mut stream = ReaderStream::with_capacity(body.compat(), STREAM_BUFFER_SIZE_BYTES);
            let mut total_bytes = 0usize;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(DownloaderError::Io)?;
                total_bytes = checked_download_total(total_bytes, chunk.len())?;
                downloaded_bytes.extend_from_slice(&chunk);
            }
            Ok::<Vec<u8>, DownloaderError>(downloaded_bytes)
        })
        .await
        .map_err(|_| DownloaderError::TimedOut)??;

        log::debug!("Download complete");
        let stringified = String::from_utf8(downloaded_bytes)
            .map_err(|e| DownloaderError::InvalidUrlBody(format!("{}", e)))?;

        Ok(stringified)
    }

    /// Download the content of a URL and block for the result.
    ///
    /// Wraps the `async` call to [`download_without_cache`] such that it can be used from sync
    /// code. This is achieved by either:
    ///
    /// 1. Reusing an existing async runtime in case one is present in the current thread, or
    /// 2. Spawning a new async runtime on the current thread
    ///
    /// If neither of these works, an error is returned instead.
    ///
    /// # Note
    ///
    /// At the moment, this function is only here to bridge the gap between the async
    /// [`Downloader`] impl and the sync [`Layout`] code that ultimately calls this function. This
    /// is needed since the Layout code can't trivially be turned `async` without a lot of
    /// refactoring, while the Downloader is used in many other places with async code and can't
    /// sensibly be sync. Maybe in the future, when more code around here is async, we can drop
    /// this function.
    pub fn download_without_cache_blocking(url: &str) -> Result<String, DownloaderError> {
        // The ephemeral runtime must outlive `block_on`, and needs its timer
        // for the download deadline.
        let ephemeral_runtime;
        let runtime_handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.clone(),
            Err(e) if e.is_missing_context() => {
                ephemeral_runtime = tokio::runtime::Builder::new_current_thread()
                    .thread_name("ephemeral runtime for downloader implementation")
                    .enable_all()
                    .build()
                    .map_err(DownloaderError::Io)?;
                ephemeral_runtime.handle().clone()
            },
            _ => {
                return Err(DownloaderError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "failed to spawn runtime for download task",
                )))
            },
        };
        runtime_handle.block_on(async move { Downloader::download_without_cache(url).await })
    }

    fn parse_name(&self, url: &str) -> Result<String, DownloaderError> {
        Url::parse(url)
            .map_err(|_| DownloaderError::NotFoundFileName(url.to_string()))?
            .path_segments()
            .ok_or_else(|| DownloaderError::NotFoundFileName(url.to_string()))?
            .last()
            .ok_or_else(|| DownloaderError::NotFoundFileName(url.to_string()))
            .map(|s| s.to_string())
    }
    async fn acquire_download_lock(&self, file_name: &String) -> Arc<Mutex<()>> {
        let mut lock_dict = self.download_locks.lock().await;
        let download_lock = lock_dict
            .entry(file_name.clone())
            .or_insert_with(|| Default::default());
        download_lock.clone()
    }
}

/// Send a GET, following up to `MAX_REDIRECTS` redirects. Every hop is
/// resolved against the current URL and re-validated with
/// `ensure_allowed_download_url` before it is requested.
async fn send_following_redirects(
    client: &HttpClient,
    url: &Url,
    range_start: Option<u64>,
) -> Result<Response<AsyncBody>, DownloaderError> {
    let mut current = url.clone();
    for _ in 0..=MAX_REDIRECTS {
        let mut request =
            Request::get(current.as_str()).header("Content-Type", "application/octet-stream");
        if let Some(start) = range_start {
            request = request.header("Range", format!("bytes={}-", start));
        }
        let res = client.send_async(request.body(())?).await?;
        if !res.status().is_redirection() {
            return Ok(res);
        }
        let location = res
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok());
        current = redirect_target(&current, location)?;
    }
    Err(DownloaderError::TooManyRedirects)
}

fn redirect_target(current: &Url, location: Option<&str>) -> Result<Url, DownloaderError> {
    let location = location.ok_or(DownloaderError::InvalidRedirect)?;
    let next = current
        .join(location)
        .map_err(|_| DownloaderError::InvalidRedirect)?;
    ensure_allowed_download_url(next.as_str())
}

/// The first byte offset of a `Content-Range: bytes START-END/TOTAL` header.
fn content_range_start<T>(res: &isahc::Response<T>) -> Option<u64> {
    let value = res.headers().get("content-range")?.to_str().ok()?;
    let range = value.trim().strip_prefix("bytes ")?;
    let (start, _) = range.split_once('-')?;
    start.trim().parse().ok()
}

/// Only a 200, or a 206 answering a resumed request, carries the artifact.
/// Other 2xx statuses (204 No Content, 205, an unrequested 206, ...) would
/// otherwise be cached as an empty or partial plugin.
fn ensure_artifact_status(status: StatusCode, resuming: bool) -> Result<(), DownloaderError> {
    match status {
        StatusCode::OK => Ok(()),
        StatusCode::PARTIAL_CONTENT if resuming => Ok(()),
        _ => Err(DownloaderError::UnexpectedStatus(status.as_u16())),
    }
}

fn checked_download_total(current: usize, additional: usize) -> Result<usize, DownloaderError> {
    let total = current
        .checked_add(additional)
        .ok_or(DownloaderError::TooLarge)?;
    if total > MAX_DOWNLOAD_BYTES {
        return Err(DownloaderError::TooLarge);
    }
    Ok(total)
}

fn ensure_allowed_download_url(url: &str) -> Result<Url, DownloaderError> {
    let parsed = Url::parse(url).map_err(|_| DownloaderError::InvalidUrl(url.to_owned()))?;
    match parsed.scheme() {
        "https" => Ok(parsed),
        "http" if parsed.host_str().is_some_and(is_loopback_host) => Ok(parsed),
        _ => Err(DownloaderError::InsecureUrl(url.to_owned())),
    }
}

fn is_loopback_host(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn the_blocking_entry_point_works_without_an_ambient_runtime() {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let url = format!("http://{}/layout.kdl", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept");
            let mut head = Vec::new();
            let mut buf = [0u8; 1024];
            while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                match socket.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => head.extend_from_slice(&buf[..n]),
                }
            }
            let _ = socket.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nlayout",
            );
        });
        assert_eq!(
            Downloader::download_without_cache_blocking(&url).expect("blocking download"),
            "layout"
        );
        server.join().unwrap();
    }

    #[test]
    fn artifact_statuses_are_accepted() {
        assert!(ensure_artifact_status(StatusCode::OK, false).is_ok());
        assert!(ensure_artifact_status(StatusCode::OK, true).is_ok());
        assert!(ensure_artifact_status(StatusCode::PARTIAL_CONTENT, true).is_ok());
    }

    #[test]
    fn non_success_statuses_are_rejected() {
        assert!(matches!(
            ensure_artifact_status(StatusCode::NOT_FOUND, false),
            Err(DownloaderError::UnexpectedStatus(404))
        ));
        assert!(matches!(
            ensure_artifact_status(StatusCode::NO_CONTENT, true),
            Err(DownloaderError::UnexpectedStatus(204))
        ));
        assert!(matches!(
            ensure_artifact_status(StatusCode::PARTIAL_CONTENT, false),
            Err(DownloaderError::UnexpectedStatus(206))
        ));
    }

    #[tokio::test]
    async fn an_empty_success_response_is_not_cached() {
        let (base, _seen) = scripted_server(|_| {
            Some("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n".to_owned())
        })
        .await;
        let location = tempdir().expect("Failed to create temp directory");
        let result = Downloader::new(location.path().to_path_buf())
            .download(&format!("{}/plugin.wasm", base), Some("plugin.wasm"))
            .await;
        assert!(
            matches!(result, Err(DownloaderError::UnexpectedStatus(204))),
            "unexpected result: {:?}",
            result
        );
        assert!(!location.path().join("plugin.wasm").exists());
    }

    #[test]
    fn download_byte_budget_is_enforced() {
        assert_eq!(
            checked_download_total(0, MAX_DOWNLOAD_BYTES).unwrap(),
            MAX_DOWNLOAD_BYTES
        );
        assert!(matches!(
            checked_download_total(MAX_DOWNLOAD_BYTES, 1),
            Err(DownloaderError::TooLarge)
        ));
    }

    #[test]
    fn only_https_or_loopback_http_downloads_are_allowed() {
        assert!(ensure_allowed_download_url("https://example.com/plugin.wasm").is_ok());
        assert!(ensure_allowed_download_url("http://localhost/plugin.wasm").is_ok());
        assert!(ensure_allowed_download_url("http://127.0.0.1/plugin.wasm").is_ok());
        assert!(ensure_allowed_download_url("http://[::1]/plugin.wasm").is_ok());
        assert!(matches!(
            ensure_allowed_download_url("http://example.com/plugin.wasm"),
            Err(DownloaderError::InsecureUrl(_))
        ));
    }

    #[tokio::test]
    async fn a_missing_http_client_is_not_reported_as_success() {
        let location = tempdir().expect("Failed to create temp directory");
        let downloader = Downloader {
            client: None,
            location: location.path().to_path_buf(),
            download_locks: Default::default(),
            deadline: DOWNLOAD_TIMEOUT,
        };

        assert!(matches!(
            downloader
                .download("https://example.com/plugin.wasm", Some("plugin.wasm"))
                .await,
            Err(DownloaderError::ClientUnavailable)
        ));
    }

    /// Minimal scripted HTTP/1.1 server on loopback. `respond` maps the
    /// raw request head to a full raw response (or `None` to hold the
    /// connection open without answering). Returns the base URL and the
    /// request heads seen so far.
    async fn scripted_server(
        respond: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
    ) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        use tokio::io::AsyncReadExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let respond = Arc::new(respond);
        let seen_in_server = seen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let respond = respond.clone();
                let seen = seen_in_server.clone();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => head.extend_from_slice(&buf[..n]),
                        }
                    }
                    let head = String::from_utf8_lossy(&head).into_owned();
                    seen.lock().unwrap().push(head.clone());
                    match respond(&head) {
                        Some(response) => {
                            let _ = socket.write_all(response.as_bytes()).await;
                            let _ = socket.shutdown().await;
                        },
                        None => {
                            // Hold the connection open, never answering.
                            tokio::time::sleep(Duration::from_secs(3600)).await;
                        },
                    }
                });
            }
        });
        (base, seen)
    }

    fn ok_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn redirect_response(location: &str) -> String {
        format!(
            "HTTP/1.1 302 Found\r\nLocation: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            location
        )
    }

    #[test]
    fn redirect_targets_are_resolved_and_revalidated() {
        let current = Url::parse("https://example.com/releases/plugin.wasm").unwrap();
        assert_eq!(
            redirect_target(&current, Some("/cdn/plugin.wasm"))
                .unwrap()
                .as_str(),
            "https://example.com/cdn/plugin.wasm"
        );
        assert_eq!(
            redirect_target(&current, Some("https://cdn.example.net/p.wasm"))
                .unwrap()
                .as_str(),
            "https://cdn.example.net/p.wasm"
        );
        assert!(matches!(
            redirect_target(&current, Some("http://example.com/plugin.wasm")),
            Err(DownloaderError::InsecureUrl(_))
        ));
        assert!(matches!(
            redirect_target(&current, None),
            Err(DownloaderError::InvalidRedirect)
        ));
    }

    #[tokio::test]
    async fn a_redirect_to_insecure_http_is_refused_before_it_is_followed() {
        let (base, seen) =
            scripted_server(|_| Some(redirect_response("http://example.com/evil.wasm"))).await;
        let result = Downloader::download_without_cache(&format!("{}/plugin.wasm", base)).await;
        assert!(
            matches!(result, Err(DownloaderError::InsecureUrl(ref u)) if u.starts_with("http://example.com/")),
            "unexpected result: {:?}",
            result
        );
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "the insecure hop must not be requested"
        );
    }

    #[tokio::test]
    async fn relative_redirects_on_an_allowed_origin_are_followed() {
        let (base, seen) = scripted_server(|head| {
            if head.starts_with("GET /final ") {
                Some(ok_response("plugin bytes"))
            } else {
                Some(redirect_response("/final"))
            }
        })
        .await;
        let body = Downloader::download_without_cache(&format!("{}/start", base))
            .await
            .expect("redirected download");
        assert_eq!(body, "plugin bytes");
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_redirected_resume_keeps_its_range() {
        let (base, seen) = scripted_server(|head| {
            let ranged = head.to_ascii_lowercase().contains("\r\nrange: bytes=4-");
            if head.starts_with("GET /final ") && ranged {
                Some(
                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 4-7/8\r\n\
                     Content-Length: 4\r\nConnection: close\r\n\r\nEBIN"
                        .to_owned(),
                )
            } else if head.starts_with("GET /plugin.wasm ") {
                Some(redirect_response("/final"))
            } else {
                Some("HTTP/1.1 500 X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned())
            }
        })
        .await;
        assert_eq!(download_over_partial(&base, b"WHOL").await, b"WHOLEBIN");
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn redirect_chains_are_bounded() {
        let (base, seen) = scripted_server(|_| Some(redirect_response("/again"))).await;
        let result = Downloader::download_without_cache(&format!("{}/start", base)).await;
        assert!(
            matches!(result, Err(DownloaderError::TooManyRedirects)),
            "unexpected result: {:?}",
            result
        );
        assert_eq!(seen.lock().unwrap().len(), MAX_REDIRECTS + 1);
    }

    #[tokio::test]
    async fn a_server_that_never_sends_headers_hits_the_deadline() {
        let (base, _seen) = scripted_server(|_| None).await;
        let result = Downloader::download_without_cache_within(
            &format!("{}/plugin.wasm", base),
            Duration::from_millis(300),
        )
        .await;
        assert!(
            matches!(result, Err(DownloaderError::TimedOut)),
            "unexpected result: {:?}",
            result
        );

        let location = tempdir().expect("Failed to create temp directory");
        let downloader = Downloader {
            client: build_client(),
            location: location.path().to_path_buf(),
            download_locks: Default::default(),
            deadline: Duration::from_millis(300),
        };
        let result = downloader
            .download(&format!("{}/plugin.wasm", base), Some("plugin.wasm"))
            .await;
        assert!(
            matches!(result, Err(DownloaderError::TimedOut)),
            "unexpected result: {:?}",
            result
        );
        assert!(!location.path().join("plugin.wasm").exists());
    }

    #[tokio::test]
    async fn an_unsatisfiable_resume_restarts_from_an_empty_file() {
        let (base, seen) = scripted_server(|head| {
            if head.to_ascii_lowercase().contains("\r\nrange:") {
                Some(
                    "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */5\r\n\
                     Content-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_owned(),
                )
            } else {
                Some(ok_response("fresh"))
            }
        })
        .await;
        let location = tempdir().expect("Failed to create temp directory");
        std::fs::write(
            location.path().join("plugin.wasm.part"),
            b"stale-and-too-long",
        )
        .expect("seed partial file");
        let downloader = Downloader::new(location.path().to_path_buf());
        downloader
            .download(&format!("{}/plugin.wasm", base), Some("plugin.wasm"))
            .await
            .expect("restarted download");
        assert_eq!(
            std::fs::read(location.path().join("plugin.wasm")).unwrap(),
            b"fresh"
        );
        assert!(!location.path().join("plugin.wasm.part").exists());
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    /// Serve the same canned response to every request carrying a Range
    /// header, and another to every request without one.
    async fn ranged_server(with_range: &'static str, without_range: &'static str) -> String {
        scripted_server(move |head| {
            let ranged = head.to_ascii_lowercase().contains("\r\nrange:");
            Some(if ranged { with_range } else { without_range }.to_owned())
        })
        .await
        .0
    }

    async fn download_over_partial(base: &str, partial: &[u8]) -> Vec<u8> {
        let location = tempdir().expect("Failed to create temp directory");
        std::fs::write(location.path().join("plugin.wasm.part"), partial).unwrap();
        Downloader::new(location.path().to_path_buf())
            .download(&format!("{}/plugin.wasm", base), Some("plugin.wasm"))
            .await
            .expect("download");
        assert!(!location.path().join("plugin.wasm.part").exists());
        std::fs::read(location.path().join("plugin.wasm")).unwrap()
    }

    #[tokio::test]
    async fn a_resume_the_server_ignores_restarts_instead_of_appending() {
        let full = "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nWHOLEBIN";
        let base = ranged_server(full, full).await;
        assert_eq!(download_over_partial(&base, b"WHOL").await, b"WHOLEBIN");
    }

    #[tokio::test]
    async fn a_honoured_resume_appends_to_the_partial_file() {
        let partial = "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 4-7/8\r\n\
                       Content-Length: 4\r\nConnection: close\r\n\r\nEBIN";
        let base = ranged_server(partial, "HTTP/1.1 500 X\r\nContent-Length: 0\r\n\r\n").await;
        assert_eq!(download_over_partial(&base, b"WHOL").await, b"WHOLEBIN");
    }

    #[tokio::test]
    async fn a_resume_answered_with_another_range_restarts() {
        let wrong_range = "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-7/8\r\n\
                           Content-Length: 8\r\nConnection: close\r\n\r\nWHOLEBIN";
        let full = "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nWHOLEBIN";
        let base = ranged_server(wrong_range, full).await;
        assert_eq!(download_over_partial(&base, b"WHOL").await, b"WHOLEBIN");
    }

    #[ignore]
    #[tokio::test]
    async fn test_download_ok() {
        let location = tempdir().expect("Failed to create temp directory");
        let location_path = location.path();

        let downloader = Downloader::new(location_path.to_path_buf());
        let result = downloader
            .download(
                "https://github.com/imsnif/monocle/releases/download/0.39.0/monocle.wasm",
                Some("monocle.wasm"),
            )
            .await
            .is_ok();

        assert!(result);
        assert!(location_path.join("monocle.wasm").exists());

        location.close().expect("Failed to close temp directory");
    }

    #[ignore]
    #[tokio::test]
    async fn test_download_without_file_name() {
        let location = tempdir().expect("Failed to create temp directory");
        let location_path = location.path();

        let downloader = Downloader::new(location_path.to_path_buf());
        let result = downloader
            .download(
                "https://github.com/imsnif/multitask/releases/download/0.38.2v2/multitask.wasm",
                None,
            )
            .await
            .is_ok();

        assert!(result);
        assert!(location_path.join("multitask.wasm").exists());

        location.close().expect("Failed to close temp directory");
    }
}
