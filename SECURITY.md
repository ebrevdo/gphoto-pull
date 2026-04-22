# Security Policy

## Reporting a Vulnerability

Do not open a public issue for vulnerabilities that expose credentials, browser
profile data, Google session details, downloaded private media, or local sync
state.

Use GitHub's private vulnerability reporting from the repository's **Security**
tab. The repository is configured to use GitHub's private reporting workflow for
security issues when that feature is available.

## Local Data

`gphoto-pull` stores browser profiles, diagnostics, downloaded media, and SQLite
state on the local machine. Treat those paths as private runtime data. Do not
attach them to issues or pull requests unless they have been reviewed and
redacted.
