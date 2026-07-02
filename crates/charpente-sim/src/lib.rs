//! Simulation implementations of the core IO traits: seeded PRNG driving IO
//! completion order, latency, virtual time, and the fault matrix — torn
//! writes, failed syncs, crash-restart between any two operations, partitions
//! (DESIGN.md §1, PLAN.md M0 step 1). A failure prints its seed; the seed is
//! the reproduction.
