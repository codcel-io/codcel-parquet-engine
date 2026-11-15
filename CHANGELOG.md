<!--
SPDX-FileCopyrightText: Copyright (c) 2026 Codcel
SPDX-License-Identifier: MIT OR Apache-2.0
-->

# Changelog

## 0.1.10 

Requires `codcel-calculation-engine` 0.1.10 and `codcel-table-engine` 0.1.10.

### Changed

- **The crate denies `clippy::unwrap_used`, `clippy::expect_used` and
  `clippy::panic`**, and production code contains none of them.
- SQL identifier validation no longer goes through a lazily compiled regex.
  Accepted identifiers are unchanged: `^[a-zA-Z_][a-zA-Z0-9_]*$`.
