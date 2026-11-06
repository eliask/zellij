<h1 align="center">
  <br>
  <img src="https://raw.githubusercontent.com/zellij-org/zellij/main/assets/logo.png" alt="logo" width="200">
  <br>
  Zellij
  <br>
  <br>
</h1>

<p align="center">
  <img src="https://raw.githubusercontent.com/zellij-org/zellij/main/assets/demo.gif" alt="demo">
</p>
<h4 align="center">
  [<a href="https://zellij.dev/documentation/installation">Installation</a>]
  [<a href="https://zellij.dev/screencasts/">Screencasts & Tutorials</a>]
  [<a href="https://zellij.dev/documentation/configuration">Configuration</a>]
  [<a href="https://zellij.dev/documentation/layouts">Layouts</a>]
  [<a href="https://zellij.dev/documentation/faq">FAQ</a>]
</h4>
<p align="center">
  <a href="https://discord.gg/CrUAFH3"><img alt="Discord Chat" src="https://img.shields.io/discord/771367133715628073?color=5865F2&label=discord&style=flat-square"></a>
  <a href="https://matrix.to/#/#zellij_general:matrix.org"><img alt="Matrix Chat" src="https://img.shields.io/matrix/zellij_general:matrix.org?color=1d7e64&label=matrix%20chat&style=flat-square&logo=matrix"></a>
  <a href="https://zellij.dev/documentation/"><img alt="Zellij documentation" src="https://img.shields.io/badge/zellij-documentation-fc0060?style=flat-square"></a>
</p>

<br>
    <p align="center">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/bc5daac4-140a-4b83-8729-71c944ee1100">
      <img src="https://github.com/user-attachments/assets/55156624-a71a-46b5-939e-f562e3b2dd7f" alt="Sponsored by ">
    </picture>
    &nbsp;
    &nbsp;
    <a href="https://www.gresearch.com/">
        <picture>
          <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/d609936a-abf8-4406-8cfc-889f76a09d74">
          <img src="https://github.com/user-attachments/assets/742ae902-fe9d-41c6-baf2-4bc143061da3" alt="gresearch logo">
        </picture>
    </a>
</p>

## About this fork

This is a fork of [Zellij](https://github.com/zellij-org/zellij), based on upstream commit `87276681`. It hardens the server, the client and the plugin host against hostile input: output from a program running in a pane (`cat` of an untrusted file, an `ssh` session to a hostile host), a misbehaving plugin, or a misbehaving client. It also fixes a number of correctness and performance problems found while doing so. Apart from the plugin changes listed below, nothing here changes how Zellij is used.

Every change lives on its own topic branch of this repository, each one small enough to review (and to cherry-pick or upstream) on its own. `main` is those branches merged together, plus this README.

### Install

With Rust 1.95 or newer and a C compiler installed:

```sh
cargo install --locked --git https://github.com/eliask/zellij zellij
```

This builds the `main` branch (it takes a while: it is a release build of the whole workspace) and installs the `zellij` binary, and `zellij-terminal`, into `~/.cargo/bin`, replacing a `zellij` installed there earlier. Add `--branch <name>` to install a single topic branch instead.

To try it without touching an existing install, give cargo another root: `cargo install --locked --root /tmp/zellij-fork --git https://github.com/eliask/zellij zellij`. A running Zellij server keeps using the binary it was started from; restart the session to pick up the new one.

### What changed

`audit/local-fixes` holds 42 independent small fixes. The other topic branches are larger changes, each focused on one area.

#### Small fixes (`audit/local-fixes`)
- Output of a pane can no longer make the server retain unbounded memory through OSC and DCS payloads, titles, hyperlinks, scroll and search loops, malformed reflows, scrolled-pane output or per-client history.
- Hyperlink anchors no longer go stale or get reused, detected links are budgeted, and an oversized wide character no longer leaves a phantom row.
- State files (resurrection layouts, session metadata) are replaced atomically; IPC frames have a size limit in both directions; a failed flush or send is reported and stops the writer.
- PTY handling: interrupted reads are retried, descriptors do not leak, a reaped child's PID is forgotten, a launch no longer panics, and a held pane's process is tracked by run.
- Clipboard helpers are fed through a bounded queue and reaped, copy commands with quoted arguments are parsed properly and a stalled helper is killed.
- Background jobs bound their timers and deduplication state; several screen paths skip redundant renders.
- Plugin host: consistent executor lock ordering, live executor thread counts, reload validated before state is changed.
- Downloads fail closed on setup errors, HTTP errors and ignored resumes; `rustls` is updated for published security fixes; the xtask invalidates plugin assets from the real inputs.

#### Hostile pane output
- `fix/pane-output-bounds`: escape sequences from a pane can no longer crash the server (the plugin-only UI component sequence is only parsed for plugin panes, a saved cursor survives the switch to the alternate screen safely, invalid scroll regions and huge repeat, insert, delete and scroll counts are clamped) or let a pane exceed its own size.
- `fix/graphics-cache-bounds`: sixel and kitty graphics are bounded while they are parsed, scaled and cached (raster size limits, chunk assembly caps, quota-aware eviction, a cap on bytes copied into one frame).
- `fix/kitty-file-mediums`: the kitty file, temporary file and shared memory transmission mediums no longer delete or read arbitrary files: paths are resolved without following symlinks, `/proc`, `/sys` and `/dev` are refused, and only a marker-named file directly in the temp directory is deleted, after it was read.
- `fix/forwarded-query-buffer`: output arriving while a pane waits for a forwarded host query is capped instead of buffered and copied without limit.
- `fix/per-pane-hyperlinks`: hyperlink ids are tracked per pane, so links keep their target when a pane moves between tabs.
- `fix/grid-scrollback-accounting`: scrollback is bounded by cells, not just rows, with cheap accounting on eviction and fast paths for scrolling and rewrapping.
- `test/hostile-output-soak`: a soak test that feeds randomized hostile output and checks that every retained structure stays bounded, plus regression tests for the bugs it found.

#### Server and clients
- `fix/client-budgets`: a byte budget for the messages queued to each client, and a watchdog that stops the writer of a client that is not reading.
- `fix/pty-writer-bounds`: the PTY writer bounds each terminal's turn and reports discarded writes as failures.
- `fix/thread-bus-fairness`: a flood of priority messages can no longer starve the other queues, including PTY output.
- `fix/pending-pane-events`: output and exit events that arrive before their pane exists are buffered within bounds and replayed.
- `fix/persistence-saves`: saves are reported only after they were published, and unchanged writes are skipped.
- `fix/command-hook-bounds`: the post-command discovery hook runs with a time, output and thread limit.
- `fix/downloader-hardening`: every redirect hop is revalidated, the whole exchange has a deadline, only complete artifact responses are accepted, and a write cut off by the deadline finishes first.
- `feat/diagnostics-counters`: counters for slow screen operations, plugin concurrency and pending PTY bytes.
- `ci/dependency-advisories`: a CI job that audits dependencies against published advisories.

#### Web client
- `fix/web-client-hardening`: revoked sessions are cut off on open websockets, other origins are refused, logins are rate limited, the number of clients, queue sizes, message sizes and terminal sizes are bounded, a slow client is dropped, and the token database is created with restricted permissions.

#### Plugins
- `fix/plugin-hardening`: plugins run with an empty environment, are cut off after a fatal callback error and on running out of fuel, must be granted permissions again after their module changes (grants are bound to the module bytes), and no longer hold a global lock while running guest code. A few non-trivial gaps are left as `TODO` comments in the code.

# What is this?

[Zellij](#origin-of-the-name) is a workspace aimed at developers, ops-oriented people and anyone who loves the terminal. Similar programs are sometimes called "Terminal Multiplexers".

Zellij is designed around the philosophy that one must not sacrifice simplicity for power, taking pride in its great experience out of the box as well as the advanced features it places at its users' fingertips.

Zellij is geared toward beginner and power users alike - allowing deep customizability, personal automation through [layouts](https://zellij.dev/documentation/layouts.html), true multiplayer collaboration, unique UX features such as floating and stacked panes, and a [plugin system](https://zellij.dev/documentation/plugins.html) allowing one to create plugins in any language that compiles to WebAssembly.

Zellij includes a built-in [web-client](https://zellij.dev/tutorials/web-client/), making a terminal optional.

You can get started by [installing](https://zellij.dev/documentation/installation.html) Zellij and checking out the [Screencasts & Tutorials](https://zellij.dev/screencasts/).

For more details about our future plans, read about upcoming features in our [roadmap](#roadmap).

## How do I install it?

The easiest way to install Zellij is through a [package for your OS](./docs/THIRD_PARTY_INSTALL.md).

If one is not available for your OS, you could download a prebuilt binary from the [latest release](https://github.com/zellij-org/zellij/releases/latest) and place it in your `$PATH`. If you'd like, we could [automatically choose one for you](#try-zellij-without-installing).

You can also install (compile) with `cargo`:

```
cargo install --locked zellij
```

#### Try Zellij without installing

bash/zsh:
```bash
bash <(curl -L https://zellij.dev/launch)
```
fish/xonsh:
```bash
bash -c 'bash <(curl -L https://zellij.dev/launch)'
```

#### Installing from `main`
Installing Zellij from the `main` branch is not recommended. This branch represents pre-release code, is constantly being worked on and may contain broken or unusable features. In addition, using it may corrupt the cache for future versions, forcing users to clear it before they can use the officially released version.

That being said - no-one will stop you from using it (and bug reports involving new features are greatly appreciated), but please consider using the latest release instead as detailed at the top of this section.

## How do I start a development environment?

* Clone the project
* In the project folder, for debug builds run: `cargo xtask run`
* To run all tests: `cargo xtask test`

For more build commands, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Configuration
For configuring Zellij, please see the [Configuration Documentation](https://zellij.dev/documentation/configuration.html).

## About issues in this repository
Issues in this repository, whether open or closed, do not necessarily indicate a problem or a bug in the software. They only indicate that the reporter wanted to communicate their experiences or thoughts to the maintainers. The Zellij maintainers do their best to go over and reply to all issue reports, but unfortunately cannot promise these will always be dealt with or even read. Your understanding is appreciated.

## Roadmap
Presented here is the project roadmap, divided into three main sections.

These are issues that are either being actively worked on or are planned for the near future.

***If you'll click on the image, you'll be led to an SVG version of it on the website where you can directly click on every issue***

[![roadmap](https://github.com/user-attachments/assets/bb55d213-4a68-4c84-ae72-7db5c9bf94fb)](https://zellij.dev/roadmap)

## Origin of the Name
[From Wikipedia, the free encyclopedia](https://en.wikipedia.org/wiki/Zellij)

Zellij (Arabic: الزليج, romanized: zillīj; also spelled zillij or zellige) is a style of mosaic tilework made from individually hand-chiseled tile pieces. The pieces were typically of different colours and fitted together to form various patterns on the basis of tessellations, most notably elaborate Islamic geometric motifs such as radiating star patterns composed of various polygons. This form of Islamic art is one of the main characteristics of architecture in the western Islamic world. It is found in the architecture of Morocco, the architecture of Algeria, early Islamic sites in Tunisia, and in the historic monuments of al-Andalus (in the Iberian Peninsula).

## License

MIT

## Sponsored by
<a href="https://terminaltrove.com/"><img src="https://avatars.githubusercontent.com/u/121595180?s=200&v=4" width="80px"></a>
<a href="https://github.com/redcathode"><img src="https://avatars.githubusercontent.com/u/75963592?s=200&v=4" width="80px"></a>
