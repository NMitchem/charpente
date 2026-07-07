# Building an app on charpente

(Stub — sections land in later commits. This file is compile-gated: every
`rust` block is built by `crates/guide-examples`.)

```rust
use charpente::{Id, table};

#[table]
pub struct Ping {
    pub id: Id<Ping>,
    pub hits: u64,
}
```
