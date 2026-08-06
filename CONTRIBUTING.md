# Contributing to Codcel Parquet Engine

Thank you for your interest in contributing to the Codcel Parquet Engine.

This repository contains the Rust implementation of Codcel's Parquet-backed table engine, providing read-only Excel-like lookup and query operations (VLOOKUP, HLOOKUP, XLOOKUP, INDEX, MATCH, FILTER, and more) over Parquet data files, with SQL query caching and request coalescing.

We welcome contributions that improve correctness, compatibility, performance, maintainability, and documentation.

---

## Scope of This Repository

This repository is intended for work related to the Rust Parquet engine, including:

- Parquet table lookup operations (VLOOKUP, HLOOKUP, XLOOKUP, INDEX, MATCH, FILTER)
- SQL query generation and execution correctness
- Query caching and request coalescing behaviour
- Sharded Parquet file support
- Type mapping and data extraction
- SQL injection protection and input validation
- Tests and compatibility validation
- Internal engine refactoring
- Developer documentation for the engine

If your issue is about the Codcel product itself rather than this specific Rust engine repository, please use the Codcel contact channels:

- General contact: https://codcel.io/contact
- Product bug reports: https://codcel.io/contact/bugs
- Feature requests: https://codcel.io/contact/features

---

## Before You Start

Before opening a Pull Request, please:

- Check whether a similar issue or pull request already exists
- Keep the change focused and limited in scope
- Prefer behaviour that matches Excel lookup semantics as closely as practical
- Add or update tests for any behaviour change
- Avoid unrelated formatting-only changes in the same PR

---

## Reporting Bugs

If you find a bug in this repository, please open a GitHub issue.

A good bug report includes:

- The lookup function or operation involved (e.g. VLOOKUP, XLOOKUP, MATCH)
- The input values and Parquet data setup used
- The actual result
- The expected result
- Whether the expected result was verified against Excel
- A minimal reproducible example
- Any relevant logs, panic messages, or failing test output

Examples of useful issue titles:

- `XLOOKUP returns incorrect result with approximate match mode`
- `Query cache returns stale result after TTL expiry`
- `FILTER fails on sharded Parquet tables with mixed types`

If the problem is in the Codcel application rather than this Rust engine, report it via:

- https://codcel.io/contact/bugs
- bugs@codcel.io

---

## Suggesting Enhancements

Enhancements are welcome.

Examples include:

- New lookup or query operation support
- Better compatibility with Excel lookup edge cases
- Performance improvements to caching or query execution
- Improved handling of Parquet data types
- Refactoring that improves readability or maintainability
- Improved test coverage
- Developer tooling improvements

For broader product ideas, language targets, or commercial feature requests, please use:

- https://codcel.io/contact/features
- features@codcel.io

---

## Contribution Workflow

All changes must come through Pull Requests.

Direct commits to protected branches should not be used except by repository owners when absolutely necessary.

Typical workflow:

1. Fork the repository
2. Create a branch for your change
3. Make your changes
4. Add or update tests
5. Run the test suite
6. Open a Pull Request

Example branch names:

- `fix-xlookup-approximate-match`
- `add-filter-sharded-tests`
- `refactor-sql-cache-cleanup`

---

## Pull Request Guidelines

Please keep Pull Requests focused and clearly explained.

Each Pull Request should ideally include:

- A short summary of the change
- The reason for the change
- The Excel lookup behaviour being matched or improved
- Notes on any edge cases
- Tests added or updated
- Any compatibility considerations

Please avoid mixing multiple unrelated changes into a single PR.

---

## Excel Compatibility Expectations

This project aims to match Excel lookup and table operation behaviour as closely as practical.

When contributing lookup or query logic:

- Prefer verified Excel behaviour over assumptions
- Document any known deviations from Excel
- Be careful with type coercion and comparison behaviour
- Consider match modes, search modes, and edge cases in lookup functions
- Consider error propagation behaviour
- Preserve backward compatibility where practical unless a bug fix requires otherwise

Where possible, include:

- Example Excel inputs and outputs
- Boundary cases
- Invalid input cases
- Cross-checks against known Excel results

---

## Tests

Tests are required for behaviour changes.

Please add or update tests when you:

- Fix a bug
- Add a lookup operation
- Change lookup or query behaviour
- Refactor logic that could affect results

Where useful, tests should include:

- Standard cases
- Edge cases
- Invalid argument cases
- Excel compatibility cases
- Regression tests for previous bugs

If the repository already has conventions for test placement or naming, follow those conventions.

Before submitting a PR, run:

```bash
cargo test
```

If applicable, also run:

```bash
cargo fmt
cargo clippy --all-targets --all-features
```

Only mention commands that actually exist in the repository workflow. If some are not currently used, keep the wording but do not add CI steps that would fail without setup.

---

## Coding Style

Please follow normal Rust best practices:

- Keep functions focused
- Prefer clear naming over cleverness
- Avoid unnecessary allocations where possible
- Keep public behaviour stable unless intentionally changing it
- Add comments where Excel behaviour is surprising or non-obvious
- Prefer small, reviewable refactors

Use `cargo fmt` formatting conventions.

---

## Documentation

If your change affects public behaviour, update relevant documentation as appropriate.

Examples:

- Supported lookup operations
- Behaviour notes
- Known limitations
- Examples
- Compatibility notes

If the change is primarily documentation-related, consider whether it belongs in `codcel-docs` instead of this repository.

---

## Confidentiality and Example Files

Do not submit confidential spreadsheets, customer models, or regulated data.

If a Parquet data example is needed:

- Use anonymized or synthetic data
- Reduce the dataset to the smallest reproducible example
- Remove sensitive business information

---

## Review and Merge Process

All Pull Requests are reviewed by a maintainer.

Maintainers may request changes for:

- correctness
- Excel compatibility
- test coverage
- code clarity
- repository scope

A Pull Request may be rejected if it:

- lacks tests for behavioural changes
- changes unrelated areas unnecessarily
- introduces unclear behaviour differences from Excel
- belongs in another repository

---

## Licensing

This project is dual licensed under the MIT License ([LICENSE-MIT](LICENSE-MIT)) and the
Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)), at the user's option.

Unless you explicitly state otherwise, any contribution you intentionally submit for
inclusion in this repository, as defined in the Apache-2.0 license, shall be dual
licensed as above, without any additional terms or conditions.

### Developer Certificate of Origin

Contributions must be signed off under the [Developer Certificate of Origin](https://developercertificate.org). Signing off certifies that you wrote the
contribution, or otherwise have the right to submit it under the licenses above.

Add the sign-off by committing with `-s`:

```bash
git commit -s -m "Fix column type mapping for Parquet decimals"
```

This appends a trailer to your commit message:

```
Signed-off-by: Your Name <your.email@example.com>
```

The name and email must match those on the commit. Pull requests whose commits are not
signed off cannot be merged.

---

## Thank You

We appreciate contributions that help improve Excel compatibility, correctness, and developer experience in the Codcel Parquet Engine.
