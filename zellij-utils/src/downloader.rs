use isahc::http::StatusCode;
use isahc::prelude::*;
use isahc::{config::RedirectPolicy, HttpClient, Request};
use std::collections::HashMap;
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
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(300);

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
    #[error("Server returned an invalid Content-Range for a resumed download")]
    InvalidContentRange,
}

#[derive(Debug, Clone)]
pub struct Downloader {
    client: Option<HttpClient>,
    location: PathBuf,
    // the whole thing is an Arc/Mutex so that Downloader is thread safe, and the individual values of
    // the HashMap are Arc/Mutexes (Mutexi?) to represent that individual downloads should not
    // happen concurrently
    download_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl Default for Downloader {
    fn default() -> Self {
        Self {
            client: HttpClient::builder()
                // TODO: timeout?
                .redirect_policy(RedirectPolicy::Follow)
                .build()
                .ok(),
            location: PathBuf::from(""),
            download_locks: Default::default(),
        }
    }
}

impl Downloader {
    pub fn new(location: PathBuf) -> Self {
        Self {
            client: HttpClient::builder()
                // TODO: timeout?
                .redirect_policy(RedirectPolicy::Follow)
                .build()
                .ok(),
            location,
            download_locks: Default::default(),
        }
    }

    pub async fn download(
        &self,
        url: &str,
        file_name: Option<&str>,
    ) -> Result<(), DownloaderError> {
        let Some(client) = &self.client else {
            log::error!("No Http client found, cannot perform requests - this is likely a misconfiguration of isahc::HttpClient");
            return Err(DownloaderError::ClientUnavailable);
        };
        let file_name = match file_name {
            Some(name) => name.to_string(),
            None => self.parse_name(url)?,
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
        let (mut target, file_part_size) = {
            if file_part_path.exists() {
                let file_part = tokio::fs::OpenOptions::new()
                    .append(true)
                    .write(true)
                    .open(&file_part_path)
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
                let file_part = tokio::fs::File::create(&file_part_path)
                    .await
                    .map_err(|e| DownloaderError::Io(e))?;

                (file_part, 0)
            }
        };
        let request = Request::get(url)
            .header("Content-Type", "application/octet-stream")
            .header("Range", format!("bytes={}-", file_part_size))
            .body(())?;
        let mut res = client.send_async(request).await?;
        let response_status = res.status();
        ensure_success_status(response_status)?;
        let mut file_part_size = file_part_size;
        if file_part_size > 0 {
            if response_status == StatusCode::PARTIAL_CONTENT {
                let content_range = res
                    .headers()
                    .get("content-range")
                    .and_then(|value| value.to_str().ok());
                validate_content_range(content_range, file_part_size)?;
            } else if response_status == StatusCode::OK {
                target = tokio::fs::File::create(&file_part_path)
                    .await
                    .map_err(DownloaderError::Io)?;
                file_part_size = 0;
            } else {
                return Err(DownloaderError::InvalidContentRange);
            }
        }
        let body = res.body_mut();
        let mut stream = ReaderStream::with_capacity(body.compat(), STREAM_BUFFER_SIZE_BYTES);
        let mut downloaded_bytes =
            usize::try_from(file_part_size).map_err(|_| DownloaderError::TooLarge)?;
        let download_result = tokio::time::timeout(DOWNLOAD_TIMEOUT, async {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(DownloaderError::Io)?;
                downloaded_bytes = checked_download_total(downloaded_bytes, chunk.len())?;
                target
                    .write_all(&chunk)
                    .await
                    .map_err(DownloaderError::Io)?;
            }
            Ok::<(), DownloaderError>(())
        })
        .await;
        match download_result {
            Ok(Ok(())) => {},
            Ok(Err(error)) => {
                if matches!(error, DownloaderError::TooLarge) {
                    let _ = tokio::fs::remove_file(&file_part_path).await;
                }
                return Err(error);
            },
            Err(_) => return Err(DownloaderError::TimedOut),
        }

        log::debug!("Download complete: {:?}", file_part_path);

        tokio::fs::rename(file_part_path, file_path)
            .await
            .map_err(|e| DownloaderError::Io(e))?;

        Ok(())
    }
    pub async fn download_without_cache(url: &str) -> Result<String, DownloaderError> {
        let request = Request::get(url)
            .header("Content-Type", "application/octet-stream")
            .body(())?;
        let client = HttpClient::builder()
            // TODO: timeout?
            .redirect_policy(RedirectPolicy::Follow)
            .build()?;

        let mut res = client.send_async(request).await?;
        ensure_success_status(res.status())?;

        let mut downloaded_bytes: Vec<u8> = vec![];
        let body = res.body_mut();
        let mut stream = ReaderStream::with_capacity(body.compat(), STREAM_BUFFER_SIZE_BYTES);
        let mut total_bytes = 0usize;
        tokio::time::timeout(DOWNLOAD_TIMEOUT, async {
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(DownloaderError::Io)?;
                total_bytes = checked_download_total(total_bytes, chunk.len())?;
                downloaded_bytes.extend_from_slice(&*chunk);
            }
            Ok::<(), DownloaderError>(())
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
        let runtime_handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.clone(),
            Err(e) if e.is_missing_context() => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .thread_name("ephemeral runtime for downloader implementation")
                    .build()
                    .map_err(DownloaderError::Io)?;
                runtime.handle().clone()
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

fn ensure_success_status(status: StatusCode) -> Result<(), DownloaderError> {
    if status.is_success() {
        Ok(())
    } else {
        Err(DownloaderError::UnexpectedStatus(status.as_u16()))
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

fn validate_content_range(
    content_range: Option<&str>,
    expected_start: u64,
) -> Result<(), DownloaderError> {
    let content_range = content_range.ok_or(DownloaderError::InvalidContentRange)?;
    let range = content_range
        .strip_prefix("bytes ")
        .and_then(|value| value.split('/').next())
        .ok_or(DownloaderError::InvalidContentRange)?;
    let start = range
        .split_once('-')
        .and_then(|(start, _end)| start.parse::<u64>().ok())
        .ok_or(DownloaderError::InvalidContentRange)?;
    if start != expected_start {
        return Err(DownloaderError::InvalidContentRange);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn success_statuses_are_accepted() {
        assert!(ensure_success_status(StatusCode::OK).is_ok());
        assert!(ensure_success_status(StatusCode::PARTIAL_CONTENT).is_ok());
    }

    #[test]
    fn non_success_statuses_are_rejected() {
        assert!(matches!(
            ensure_success_status(StatusCode::NOT_FOUND),
            Err(DownloaderError::UnexpectedStatus(404))
        ));
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
    fn resumed_download_requires_matching_content_range() {
        assert!(validate_content_range(Some("bytes 100-199/200"), 100).is_ok());
        assert!(matches!(
            validate_content_range(Some("bytes 0-99/200"), 100),
            Err(DownloaderError::InvalidContentRange)
        ));
        assert!(matches!(
            validate_content_range(None, 100),
            Err(DownloaderError::InvalidContentRange)
        ));
    }

    #[tokio::test]
    async fn a_missing_http_client_is_not_reported_as_success() {
        let location = tempdir().expect("Failed to create temp directory");
        let downloader = Downloader {
            client: None,
            location: location.path().to_path_buf(),
            download_locks: Default::default(),
        };

        assert!(matches!(
            downloader
                .download("https://example.com/plugin.wasm", Some("plugin.wasm"))
                .await,
            Err(DownloaderError::ClientUnavailable)
        ));
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
