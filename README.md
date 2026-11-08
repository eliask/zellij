## About this fork

This is a fork of [Zellij](https://github.com/zellij-org/zellij). Its goal is to fix every correctness, security, performance/algorithmic-complexity and freeze/hang issue that can be found and removed without a behavioral tradeoff. That includes hardening the server, the client and the plugin host against hostile input: output from a program running in a pane (`cat` of an untrusted file, an `ssh` session to a hostile host), a misbehaving plugin, or a misbehaving client.

The fork is the result of repeated audits of every subsystem rather than a single patch series. Each issue is fixed at its root, covered by a regression test where one can be written, and kept bounded under adversarial input. Remaining tradeoffs are documented rather than hidden.

Some of that hardening is visible. A character wider than the pane is clipped while the pane is that narrow and restored when the pane grows, instead of being kept on a row wider than the pane. Output and exits that arrive before a pane exists are bounded; past the cap, further panes are not started, and output that does not fit is cut off with an explicit notice rather than replayed as a suffix. Kitty file, temporary-file and shared-memory image transfers from pane output are refused; direct image data still works. Plugin downloads are not resumed from a partial file. A plugin callback that asks the host to wait is cut off with an error when its time budget runs out, rather than reporting that the timer elapsed. Scrolling still uses the existing catch-up path: it has been bounded, not replaced by an independent view.

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
- Hyperlink anchors no longer go stale or get reused, and detected links are budgeted. An oversized wide character is clipped to the pane and restored when the pane is wide enough again, instead of leaving a row wider than the pane.
- State files (resurrection layouts, session metadata) are replaced atomically; IPC frames have a size limit in both directions; a failed flush or send is reported and stops the writer.
- PTY handling: interrupted reads are retried, descriptors do not leak, a reaped child's PID is forgotten, a launch no longer panics, and a held pane's process is tracked by run.
- Clipboard helpers are fed through a bounded queue and reaped, copy commands with quoted arguments are parsed properly and a stalled helper is killed.
- Background jobs bound their timers and deduplication state; several screen paths skip redundant renders.
- Plugin host: consistent executor lock ordering, live executor thread counts, reload validated before state is changed.
- Downloads fail closed on setup errors and HTTP errors, and a partial download is not resumed; `rustls` is updated for published security fixes; the xtask invalidates plugin assets from the real inputs.

#### Hostile pane output
- `fix/pane-output-bounds`: escape sequences from a pane can no longer crash the server (the plugin-only UI component sequence is only parsed for plugin panes, a saved cursor survives the switch to the alternate screen safely, invalid scroll regions and huge repeat, insert, delete and scroll counts are clamped) or let a pane exceed its own size.
- `fix/graphics-cache-bounds`: sixel and kitty graphics are bounded while they are parsed, scaled and cached (raster size limits, chunk assembly caps, quota-aware eviction, a cap on bytes copied into one frame).
- `fix/kitty-file-mediums`: kitty file, temporary-file and shared-memory transfers are refused. Pane output cannot ask the host to read or delete a local file. Direct (`t=d`) image data still works. The unix open and delete helpers remain tested, and they are not what a pane can invoke.
- `fix/forwarded-query-buffer`: output arriving while a pane waits for a forwarded host query is capped instead of buffered and copied without limit.
- `fix/per-pane-hyperlinks`: hyperlink ids are tracked per pane, so links keep their target when a pane moves between tabs.
- `fix/grid-scrollback-accounting`: scrollback is bounded by cells, not just rows, with cheap accounting on eviction and fast paths for scrolling and rewrapping.
- `test/hostile-output-soak`: a soak test that feeds randomized hostile output and checks that every retained structure stays bounded, plus regression tests for the bugs it found.

#### Server and clients
- `fix/client-budgets`: a byte budget for the messages queued to each client, and a watchdog that stops the writer of a client that is not reading.
- `fix/pty-writer-bounds`: the PTY writer bounds each terminal's turn and reports discarded writes as failures.
- `fix/thread-bus-fairness`: a flood of priority messages can no longer starve the other queues, including PTY output.
- `fix/pending-pane-events`: output and exit events that arrive before their pane exists are buffered within bounds and replayed. A pane is not spawned once too many are still waiting to be created. Buffered output keeps its prefix; what does not fit is dropped and the pane is told it was cut off. An exit or hold for an admitted pane is not discarded to make room.
- `fix/persistence-saves`: saves are reported only after they were published, and unchanged writes are skipped.
- `fix/command-hook-bounds`: the post-command discovery hook runs with a time, output and thread limit.
- `fix/downloader-hardening`: every redirect hop is revalidated, the whole exchange has a deadline, only a complete HTTP 200 is published, and each attempt writes a unique temporary file. Partial files are not resumed, so a changed object at the same URL cannot be spliced onto an old prefix.
- `feat/diagnostics-counters`: counters for slow screen operations, plugin concurrency and pending PTY bytes. The screen operation in progress is readable while it is still running, so a stuck operation is visible before its guard is dropped.
- `ci/dependency-advisories`: a CI job that audits dependencies against published advisories.

#### Web client
- `fix/web-client-hardening`: revoked sessions are cut off on open websockets, other origins are refused, logins are rate limited, the number of clients, queue sizes, message sizes and terminal sizes are bounded, a slow client is dropped, and the token database is created with restricted permissions.

#### Plugins
- `fix/plugin-hardening`: plugins run with an empty environment, are cut off after a fatal callback error and on running out of fuel, must be granted permissions again after their module changes (grants are bound to the module bytes), and no longer hold a global lock while running guest code. A callback also has one wall-clock budget for host waits. `poll_oneoff` does not rewrite the guest's clock subscriptions or report a timer that has not elapsed; past the budget it returns `EINTR`. Other blocking host calls are not all covered yet (`TODO` comments in the code).

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
