#!/usr/bin/env bash
# Regression for pinned, verified protoc installs and pinned workflow inputs.
# shellcheck disable=SC2016  # patterns are literal installer text, not expansions
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
script="$root/.github/scripts/install-protoc.sh"
cd "$root"

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

[[ -f "$script" ]] || fail "missing $script"

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

assert_script_pins() {
    grep -F 'readonly protoc_version="23.4"' "$script" >/dev/null \
        || fail "protoc is not pinned to 23.4"
    grep -F 'Linux)' "$script" >/dev/null || fail "missing Linux installer path"
    grep -F 'macOS|Darwin)' "$script" >/dev/null || fail "missing macOS installer path"
    grep -F 'Windows|MINGW*|MSYS*|CYGWIN*)' "$script" >/dev/null \
        || fail "missing Windows installer path"
    grep -F 'asset="protoc-${protoc_version}-linux-x86_64.zip"' "$script" >/dev/null \
        || fail "missing linux asset"
    grep -F 'expected_sha256="0502f286ac9ed860b629a7965a14527b1f2dd131e4283fa23c2d7f184672aa9a"' "$script" >/dev/null \
        || fail "missing linux checksum"
    grep -F 'asset="protoc-${protoc_version}-osx-universal_binary.zip"' "$script" >/dev/null \
        || fail "missing macOS asset"
    grep -F 'expected_sha256="123d184419b1b83b36e463bc7ee8345b4376e691af0865b4e43c7d8ac459f972"' "$script" >/dev/null \
        || fail "missing macOS checksum"
    grep -F 'asset="protoc-${protoc_version}-win64.zip"' "$script" >/dev/null \
        || fail "missing Windows asset"
    grep -F 'expected_sha256="a309c39442fb75f0db343cb22c111a00f91cdf0767f332e170644b9378e2bcc6"' "$script" >/dev/null \
        || fail "missing Windows checksum"
    # Checksum verification stays ahead of extraction in the script text.
    checksum_line=$(grep -n 'sha256sum --check' "$script" | head -n 1 | cut -d: -f1)
    unzip_line=$(grep -n 'unzip -q' "$script" | head -n 1 | cut -d: -f1)
    [[ -n "$checksum_line" && -n "$unzip_line" && "$checksum_line" -lt "$unzip_line" ]] \
        || fail "checksum check is not before unzip"
}

asset_for_os() {
    case "$1" in
        Linux) printf '%s\n' "protoc-23.4-linux-x86_64.zip" ;;
        macOS | Darwin) printf '%s\n' "protoc-23.4-osx-universal_binary.zip" ;;
        Windows | MINGW* | MSYS* | CYGWIN*) printf '%s\n' "protoc-23.4-win64.zip" ;;
        *) return 1 ;;
    esac
}

prepare_stubs() {
    local bin="$1"
    mkdir -p "$bin"
    cat >"$bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
output=""
args=("$@")
printf '%s\n' "${args[@]}" >"$CURL_LOG"
i=0
while [[ $i -lt ${#args[@]} ]]; do
    if [[ "${args[$i]}" == "--output" ]]; then
        output="${args[$((i + 1))]}"
    fi
    i=$((i + 1))
done
[[ -n "$output" ]] || exit 2
if [[ "$STUB_MODE" == "mismatch" ]]; then
    printf 'not-a-protoc-archive\n' >"$output"
    exit 0
fi
if [[ "$STUB_MODE" == "missing-checksum-tool" ]]; then
    printf 'not-a-protoc-archive\n' >"$output"
    exit 0
fi
url="${args[-1]}"
asset="${url##*/}"
cp "$REPLAY_DIR/$asset" "$output"
EOF
    cat >"$bin/unzip" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$@" >>"$UNZIP_LOG"
exec /bin/unzip "$@"
EOF
    chmod 755 "$bin/curl" "$bin/unzip"
}

run_installer() {
    local os="$1"
    local mode="$2"
    local tmp="$scratch/run-${os}-${mode}"
    local path_prefix
    rm -rf "$tmp"
    mkdir -p "$tmp/bin" "$tmp/work"
    prepare_stubs "$tmp/bin"
    : >"$tmp/github_path"
    if [[ "$mode" == "missing-checksum-tool" ]]; then
        path_prefix="$tmp/bin"
    else
        path_prefix="$tmp/bin:$PATH"
    fi
    set +e
    CURL_LOG="$tmp/curl.args" \
        UNZIP_LOG="$tmp/unzip.log" \
        STUB_MODE="$mode" \
        REPLAY_DIR="$scratch/replay" \
        RUNNER_OS="$os" \
        RUNNER_TEMP="$tmp/work" \
        GITHUB_PATH="$tmp/github_path" \
        PATH="$path_prefix" \
        bash "$script" >"$tmp/stdout" 2>"$tmp/stderr"
    local rc=$?
    set -e
    printf '%s\n' "$rc"
}

assert_requested_asset() {
    local os="$1"
    local tmp="$2"
    local asset url
    asset=$(asset_for_os "$os")
    url=$(tail -n 1 "$tmp/curl.args")
    [[ "$url" == "https://github.com/protocolbuffers/protobuf/releases/download/v23.4/${asset}" ]] \
        || fail "$os requested '$url', want asset $asset"
    grep -F -- "--proto" "$tmp/curl.args" >/dev/null || fail "$os download is not restricted to a protocol"
    grep -F -- "=https" "$tmp/curl.args" >/dev/null || fail "$os download is not HTTPS-only"
}

test_mismatch_rejects_before_extraction() {
    local os tmp rc
    for os in Linux macOS Darwin Windows MINGW64_NT-10.0 MSYS_NT-10.0 CYGWIN_NT-10.0; do
        rc=$(run_installer "$os" mismatch)
        tmp="$scratch/run-${os}-mismatch"
        [[ "$rc" -ne 0 ]] || fail "$os accepted a checksum mismatch"
        [[ ! -s "$tmp/unzip.log" ]] || fail "$os extracted before checksum verification"
        [[ ! -e "$tmp/work/protoc-23.4" ]] || fail "$os created an install directory after a bad checksum"
        assert_requested_asset "$os" "$tmp"
    done

    rc=$(run_installer Plan9 mismatch)
    tmp="$scratch/run-Plan9-mismatch"
    [[ "$rc" -ne 0 ]] || fail "unsupported OS was accepted"
    [[ ! -e "$tmp/curl.args" ]] || fail "unsupported OS downloaded an archive"
    [[ ! -s "$tmp/unzip.log" ]] || fail "unsupported OS extracted an archive"

    rc=$(run_installer Linux missing-checksum-tool)
    tmp="$scratch/run-Linux-missing-checksum-tool"
    [[ "$rc" -ne 0 ]] || fail "installer ran without a checksum tool"
    [[ ! -s "$tmp/unzip.log" ]] || fail "installer extracted when no checksum tool exists"
}

fetch_official_assets() {
    local asset
    mkdir -p "$scratch/replay"
    for asset in \
        protoc-23.4-linux-x86_64.zip \
        protoc-23.4-osx-universal_binary.zip \
        protoc-23.4-win64.zip; do
        curl --fail --silent --show-error --location --proto '=https' \
            --retry 3 --connect-timeout 15 \
            --output "$scratch/replay/$asset" \
            "https://github.com/protocolbuffers/protobuf/releases/download/v23.4/${asset}"
    done
    [[ $(sha256sum "$scratch/replay/protoc-23.4-linux-x86_64.zip" | awk '{print $1}') == "0502f286ac9ed860b629a7965a14527b1f2dd131e4283fa23c2d7f184672aa9a" ]] \
        || fail "official linux asset does not match the pinned checksum"
    [[ $(sha256sum "$scratch/replay/protoc-23.4-osx-universal_binary.zip" | awk '{print $1}') == "123d184419b1b83b36e463bc7ee8345b4376e691af0865b4e43c7d8ac459f972" ]] \
        || fail "official macOS asset does not match the pinned checksum"
    [[ $(sha256sum "$scratch/replay/protoc-23.4-win64.zip" | awk '{print $1}') == "a309c39442fb75f0db343cb22c111a00f91cdf0767f332e170644b9378e2bcc6" ]] \
        || fail "official Windows asset does not match the pinned checksum"
}

test_verified_install() {
    local os tmp rc binary
    fetch_official_assets
    for os in Linux macOS Darwin Windows MINGW64_NT-10.0 MSYS_NT-10.0 CYGWIN_NT-10.0; do
        rc=$(run_installer "$os" replay)
        tmp="$scratch/run-${os}-replay"
        [[ "$rc" -eq 0 ]] || fail "$os rejected the official archive (see $tmp/stderr)"
        [[ -s "$tmp/unzip.log" ]] || fail "$os did not extract a verified archive"
        assert_requested_asset "$os" "$tmp"
        case "$os" in
            Windows | MINGW* | MSYS* | CYGWIN*) binary="protoc.exe" ;;
            *) binary="protoc" ;;
        esac
        [[ -f "$tmp/work/protoc-23.4/bin/$binary" ]] || fail "$os did not install bin/$binary"
        grep -F "$tmp/work/protoc-23.4/bin" "$tmp/github_path" >/dev/null \
            || fail "$os did not publish the protoc bin directory"
    done
}

test_workflows() {
    local workflow line ref
    for workflow in .github/workflows/rust.yml .github/workflows/e2e.yml .github/workflows/release.yml; do
        [[ -f "$workflow" ]] || fail "missing $workflow"
        if grep -Eq 'protoc-23\.4-|protocolbuffers/protobuf/releases' "$workflow"; then
            fail "$workflow still downloads protoc inline"
        fi
        grep -F 'tr -d '\''\r'\'' < .github/scripts/install-protoc.sh' "$workflow" >/dev/null \
            || fail "$workflow does not normalize the installer script"
        grep -F 'bash "$RUNNER_TEMP/install-protoc.sh"' "$workflow" >/dev/null \
            || fail "$workflow does not run the verified installer"
        while IFS= read -r line; do
            ref=$(printf '%s\n' "$line" | sed -n 's/.*uses:[[:space:]]*//p' | awk '{print $1}')
            [[ -n "$ref" ]] || continue
            [[ "$ref" =~ ^[^@[:space:]]+@[0-9a-f]{40}$ ]] \
                || fail "$workflow has unpinned executable input: $ref"
        done < <(grep 'uses:' "$workflow")
    done
    grep -F 'image: ghcr.io/linuxserver/openssh-server@sha256:161e0b670b31d9539ca1718bc5943e981779331b4e1c9d32e8929d7652a1f22f' \
        .github/workflows/e2e.yml >/dev/null \
        || fail "e2e OpenSSH image is not pinned"
    if grep -q 'docker://' .github/workflows/e2e.yml .github/workflows/rust.yml .github/workflows/release.yml; then
        fail "a workflow still executes an unpinned docker:// image"
    fi
}

assert_script_pins
test_workflows
test_mismatch_rejects_before_extraction
test_verified_install
printf 'ok\n'
