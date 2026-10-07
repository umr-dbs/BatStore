# BTW artifact binary

This directory contains a prebuilt BatStore executable for artifact review when
building the Rust project is inconvenient.

## Compatibility

- Linux on x86-64
- Linux kernel 3.2 or newer
- No AVX2 requirement: the binary was compiled for the baseline x86-64 CPU and
  selects the AVX2 implementation at runtime when the processor supports it.

The executable is a static position-independent binary and has no shared-library
dependencies.

## Verify and run

From the repository root:

```console
sha256sum --check artifacts/btw/SHA256SUMS
./artifacts/btw/linux-x86_64/batstore
```

The second command is a smoke test and prints the BatStore banner followed by
the command prompt. Workload arguments are the same as for the locally built
`target/release/batstore` executable.

## Build provenance

- Source commit: `2f870ac4f79162bb743b0e7234e0384ae94304d2`
- Package version: `0.0.142`
- Rust: `rustc 1.99.0 (b940084d7 2026-09-28)`
- Profile: `release`
- Cargo features: `mdbx-backend`
- Default allocator: jemalloc

The binary was built from the repository root with:

```console
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64 -C target-feature=+crt-static" \
  cargo build --release --locked --features mdbx-backend --bin batstore \
  --target x86_64-unknown-linux-gnu
```

The target-specific flags override the repository's performance-oriented
`target-cpu=native` setting and request static C runtime linkage. For paper
measurements, build on the evaluation machine using the normal setup
instructions instead; the prebuilt binary is a convenience and
functional-validation artifact, not the reference performance configuration.
