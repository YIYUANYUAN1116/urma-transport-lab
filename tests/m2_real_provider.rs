#![cfg(feature = "urma")]

use std::{
    io::Write,
    process::{Command, Stdio},
    thread,
    time::Duration,
};

/// Requires a real provider, a usable EID 0, and permission to initialize
/// liburma in two processes. It deliberately exercises the public binaries.
#[test]
#[ignore = "requires a real URMA provider and hardware"]
fn parent_child_static_oob_reaches_ready() {
    let device = std::env::var("URMA_TEST_DEVICE").unwrap_or_else(|_| "urma0".into());
    let address = "127.0.0.1:31909";
    let mut parent = Command::new(env!("CARGO_BIN_EXE_parent"))
        .args([&device, address])
        .spawn()
        .expect("start parent");
    thread::sleep(Duration::from_millis(500));
    let mut child = Command::new(env!("CARGO_BIN_EXE_child"))
        .args([&device, address])
        .stdin(Stdio::piped())
        .spawn()
        .expect("start child");
    child
        .stdin
        .as_mut()
        .expect("child stdin")
        .write_all(b"\n")
        .expect("release child");
    assert!(child.wait().expect("wait child").success());
    assert!(parent.wait().expect("wait parent").success());
}
