# Contributing to zarrs

Thank you for your interest in contributing to `zarrs`!

## How to Contribute

*   **Issues:** Report bugs or suggest enhancements by opening an issue on GitHub. Please check existing issues first.
*   **Pull Requests (PRs):** We welcome code and documentation contributions via PRs.

## Issue Guidelines

When opening an issue, please provide:

- **Clear Title:** A brief summary of the issue.
- **Bug Reports:** Include the `zarrs` version, steps to reproduce, expected vs. actual behavior, and any relevant logs or errors.
- **Feature Requests:** Describe the feature, its use case, and why it's needed.

## Pull Request Guidelines

> [!IMPORTANT]
> For a major change, please open an issue or start a discussion first to discuss the proposal.

- **Add Tests**: Add tests for any new functionality or bug fixes.
- **Code Quality**: Run `make check` to format your code, run clippy, and ensure all tests pass. Or alternatively:
  - Format your code using `cargo fmt`.
  - Lint with `cargo clippy --all-features -- -D warnings`.
  - Run tests using `cargo test --all-features`.
- **Changelog**: Please include a suggested entry for the `CHANGELOG.md` in your PR description if the PR title is not sufficient.

> [!NOTE]
> CI will run additional checks across multiple platforms and check semver and minimal versions.

---

Thanks again for contributing!
