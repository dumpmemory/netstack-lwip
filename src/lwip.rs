#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(improper_ctypes)]
#![allow(dead_code)]
#![allow(clippy::all)]

// Bindings are generated at build time for the current target (see build.rs)
// and written to OUT_DIR, so their ABI always matches the target.
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
