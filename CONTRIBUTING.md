# Contributing to pipecat-freeswitch

Thank you for your interest in contributing! 🎉 Every contribution makes a difference: fixing a bug, testing with
another FreeSWITCH version or phone, improving the documentation or proposing a feature.

Please read the guidelines below so collaboration stays efficient and pleasant for everyone.

---

## 📌 Code of conduct

This project follows the [Contributor Covenant, version 2.1](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).
Be respectful, welcoming and constructive; harassment is not tolerated. Report unacceptable behavior to the
maintainer at marujofln@gmail.com.

---

## 🚀 Getting started

If you are new to the project, look for issues labeled:

* `good first issue`: smaller tasks, good for a first contribution;
* `help wanted`: tasks where help is most needed.

Testing with setups we have not covered is also very valuable: other FreeSWITCH versions, codecs (G.722, G.729),
phones and softphones, and the AI services that `examples/bot.py` supports but has not run live against yet.

### 💻 Local setup

Requirements: Python 3.12 or later and [uv](https://docs.astral.sh/uv/). Docker is needed only for the FreeSWITCH
test bench.

1. **Fork** this repository on GitHub.
2. Clone your fork:
   ```bash
   git clone https://github.com/<your-user>/pipecat-freeswitch.git
   cd pipecat-freeswitch
   ```
3. Install the package and the development dependencies:
   ```bash
   uv sync --group dev
   ```
4. Create a branch for your change:
   ```bash
   git checkout -b fix/short-description
   ```

The unit and integration tests need no FreeSWITCH. To try changes against a real FreeSWITCH, use the bench in
[`examples/freeswitch/`](examples/freeswitch/README.md) (see "Development" in the [README](README.md#development)).
The bench uses fixed, public credentials: run it on a trusted network only.

---

## 🐛 Reporting bugs

Found a problem? These steps help us fix it:

1. Check that it has not been reported already in the [issues](https://github.com/marujofln/pipecat-freeswitch/issues).
2. If it is new, open an issue with:
   * a clear description of the problem;
   * exact steps to reproduce it (the dialplan, and the serializer and transport parameters you use);
   * the expected behavior and what happened instead;
   * versions: `pipecat-freeswitch`, Pipecat, FreeSWITCH, the mod_audio_stream commit, Python, and the codec of
     the call (`fs_cli -x "uuid_dump <uuid>"` shows `write_codec` and `write_rate`);
   * relevant logs: the bot's log and FreeSWITCH's (`fs_cli` with `/log debug`).

**Remove secrets before posting:** API keys, event socket passwords, WebSocket tokens, and call recordings or
transcripts with other people's voices or data.

**Security vulnerabilities must not be reported in public issues.** Follow [SECURITY.md](SECURITY.md) instead.

---

## 💡 Suggesting improvements

Have an idea for the project?

1. Open an issue describing the feature you want to propose.
2. Explain **why** it is useful and who benefits from it (which deployment, codec or use case).
3. Wait for feedback from the maintainer before writing code, so the idea fits the project's scope and nobody
   redoes work. The planned items are in the roadmap of [`docs/specification.md`](docs/specification.md#roadmap),
   for example DTMF support and a growing-block chunker.

---

## 🛠️ Code and commit standards

To keep the code consistent with Pipecat and with the rest of the project:

* **Language:** everything in English: identifiers, comments, docstrings, documentation and commit messages.
* **Style:** [Ruff](https://docs.astral.sh/ruff/) for linting and formatting (100 columns; the bench uses 120).
  Run before sending your changes:
  ```bash
  uv run ruff check .
  uv run ruff format src tests bench examples
  ```
* **Docstrings:** Google style, following
  [Pipecat's docstring conventions](https://github.com/pipecat-ai/pipecat/blob/main/CONTRIBUTING.md#docstring-conventions)
  for public classes and methods.
* **Tests:** new features and bug fixes come with tests that fail without the change. All tests must pass, and CI
  requires 95% branch coverage:
  ```bash
  uv run pytest --cov                 # unit and integration tests
  uv run pytest -m bench              # optional: end-to-end calls through the running FreeSWITCH bench
  ```
* **Measurements over assumptions:** behavior that depends on FreeSWITCH or on audio (timing, sample rates,
  playback, barge-in) should be checked on the bench, and the numbers belong in the PR description and, when they
  change a decision, in [`docs/specification.md`](docs/specification.md).
* **Commit messages:** a short imperative summary line (for example, `Validate the channel UUID before using it in
  ESL commands`), a blank line, then a body explaining what changed and **why**, with measurements when relevant.
  One logical change per commit.

---

## 🔀 Pull request process

When your change is ready:

1. Push your branch to your fork:
   ```bash
   git push origin fix/short-description
   ```
2. Open a **pull request** against the `main` branch of this repository.
3. Describe what changed and why, how you tested it, and link the related issue.
4. Make sure the CI checks pass (lint, tests on Python 3.12 and 3.13, package contents).
5. Wait for the maintainer's review and keep an eye on the comments in case changes are requested.

---

## 📝 Pull request checklist

Before submitting your PR, check that:

- [ ] My code follows the project's style (`ruff check` and `ruff format` pass).
- [ ] I reviewed my own changes.
- [ ] I added tests that prove the fix or the feature works.
- [ ] All new and existing tests pass locally (`uv run pytest --cov`).
- [ ] I updated the relevant documentation (README, `docs/specification.md`, docstrings), if applicable.
- [ ] I added an entry under "Unreleased" in [CHANGELOG.md](CHANGELOG.md), if users will notice the change.
- [ ] I removed secrets and personal data from code, logs and test files.

---

Thank you for helping build better software!
