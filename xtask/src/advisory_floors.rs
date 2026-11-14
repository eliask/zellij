//! Locked-crate floors for the RustSec advisories that apply to this tree.
//! Versions may move forward. They may not fall back into an affected range.

use std::cmp::Ordering;
use std::fs;
use std::path::Path;

#[cfg(test)]
fn workspace_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn parse_version(version: &str) -> Vec<u64> {
    version
        .split('+')
        .next()
        .unwrap_or(version)
        .split('.')
        .map(|part| {
            part.chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .unwrap_or(0)
        })
        .collect()
}

fn cmp_version(left: &[u64], right: &[u64]) -> Ordering {
    let len = left.len().max(right.len());
    for index in 0..len {
        let ordering = left
            .get(index)
            .copied()
            .unwrap_or(0)
            .cmp(&right.get(index).copied().unwrap_or(0));
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

fn at_least(version: &[u64], floor: &str) -> bool {
    cmp_version(version, &parse_version(floor)) != Ordering::Less
}

fn below(version: &[u64], upper: &str) -> bool {
    cmp_version(version, &parse_version(upper)) == Ordering::Less
}

/// Every locked copy of `package` is at least `floor`.
fn floor_holds(lock: &str, package: &str, floor: &str) -> bool {
    let versions = locked_versions(lock, package);
    !versions.is_empty() && versions.iter().all(|version| at_least(version, floor))
}

fn locked_versions(lock: &str, package: &str) -> Vec<Vec<u64>> {
    let mut versions = Vec::new();
    let mut lines = lock.lines();
    let name_line = format!("name = \"{package}\"");
    while let Some(line) = lines.next() {
        if line != "[[package]]" {
            continue;
        }
        let name = lines.next().unwrap_or("");
        let version = lines.next().unwrap_or("");
        if name != name_line {
            continue;
        }
        let Some(version) = version
            .strip_prefix("version = \"")
            .and_then(|rest| rest.strip_suffix('"'))
        else {
            continue;
        };
        versions.push(parse_version(version));
    }
    versions
}

fn rand_is_patched(version: &[u64]) -> bool {
    below(version, "0.7.0")
        || (at_least(version, "0.8.6") && below(version, "0.9.0"))
        || (at_least(version, "0.9.3") && below(version, "0.10.0"))
        || at_least(version, "0.10.1")
}

fn rustix_is_patched(version: &[u64]) -> bool {
    below(version, "1.0.0") || at_least(version, "1.1.6")
}

fn tokio_is_patched(version: &[u64]) -> bool {
    below(version, "0.3.2")
        || (at_least(version, "1.51.6") && below(version, "1.52.0"))
        || at_least(version, "1.53.3")
}

fn scc_is_patched(versions: &[Vec<u64>]) -> bool {
    versions.iter().all(|version| at_least(version, "3.8.4"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locked_dependencies_stay_above_the_advisory_floors() {
        let root = workspace_root();
        let lock = fs::read_to_string(root.join("Cargo.lock")).expect("Cargo.lock");
        let manifest = fs::read_to_string(root.join("zellij-utils/Cargo.toml"))
            .expect("zellij-utils Cargo.toml");

        // RUSTSEC-2026-0459: rusqlite 0.41 brings bundled SQLite 3.54.
        // `cache` keeps `prepare_cached`, which 0.30 exposed by default.
        assert!(
            manifest.contains("version = \"0.41\"")
                && manifest.contains("\"bundled\"")
                && manifest.contains("\"cache\""),
            "rusqlite must stay on 0.41 with the bundled and cache features"
        );

        for (package, floor) in [
            ("anyhow", "1.0.103"),         // RUSTSEC-2026-0190
            ("cap-primitives", "3.4.6"),   // RUSTSEC-2026-0482
            ("cap-std", "3.4.6"),          // same cap-std release
            ("cap-fs-ext", "3.4.6"),       // same cap-std release
            ("crossbeam-epoch", "0.9.20"), // RUSTSEC-2026-0204
            ("h2", "0.4.16"),              // RUSTSEC-2026-0258
            ("libsqlite3-sys", "0.39.0"),  // RUSTSEC-2026-0459
            ("libz-sys", "1.1.31"),        // RUSTSEC-2026-0418
            ("rustls", "0.23.46"),         // RUSTSEC-2026-0285/0346/0348/0349
            ("rustls-webpki", "0.103.16"), // RUSTSEC-2026-0382
            ("rusqlite", "0.41.0"),        // brings the patched SQLite
            ("chacha20", "0.10.2"),        // 0.10.1 is yanked
            ("clipboard-win", "5.4.3"),    // 5.4.1 is yanked
        ] {
            assert!(
                floor_holds(&lock, package, floor),
                "{package} is behind {floor}: {:?}",
                locked_versions(&lock, package)
            );
        }

        let rand = locked_versions(&lock, "rand");
        assert!(
            !rand.is_empty() && rand.iter().all(|version| rand_is_patched(version)),
            "rand is inside RUSTSEC-2026-0097: {rand:?}"
        );
        let rustix = locked_versions(&lock, "rustix");
        assert!(
            !rustix.is_empty() && rustix.iter().all(|version| rustix_is_patched(version)),
            "rustix is inside RUSTSEC-2026-0486: {rustix:?}"
        );
        let tokio = locked_versions(&lock, "tokio");
        assert!(
            !tokio.is_empty() && tokio.iter().all(|version| tokio_is_patched(version)),
            "tokio is inside RUSTSEC-2026-0386: {tokio:?}"
        );
        assert!(
            scc_is_patched(&locked_versions(&lock, "scc")),
            "scc is inside RUSTSEC-2026-0205"
        );
    }
}
