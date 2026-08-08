#![cfg(feature = "urma")]

use std::{process::Command, thread, time::Duration};

/// Requires a real provider, a usable EID 0, and permission to initialize
/// liburma in two processes. The binaries verify READY + URMA Ping/Pong.
#[test]
#[ignore = "requires a real URMA provider and hardware"]
fn parent_child_urma_ping_pong() {
    let device = std::env::var("URMA_TEST_DEVICE").unwrap_or_else(|_| "urma0".into());
    let address = "127.0.0.1:31909";
    let mut parent = Command::new(env!("CARGO_BIN_EXE_parent"))
        .args([&device, address])
        .spawn()
        .expect("start parent");
    thread::sleep(Duration::from_millis(500));
    let child = Command::new(env!("CARGO_BIN_EXE_child"))
        .args([&device, address])
        .status()
        .expect("run child");
    assert!(child.success());
    assert!(parent.wait().expect("wait parent").success());
}

#[test]
#[ignore = "requires a real URMA provider and hardware"]
fn parent_child_urma_ping_pong_one_hundred_rounds() {
    let device = std::env::var("URMA_TEST_DEVICE").unwrap_or_else(|_| "urma0".into());
    let address = "127.0.0.1:31910";
    let mut parent = Command::new(env!("CARGO_BIN_EXE_parent"))
        .args([&device, address, "100"])
        .spawn()
        .expect("start parent");
    thread::sleep(Duration::from_millis(500));
    let child = Command::new(env!("CARGO_BIN_EXE_child"))
        .args([&device, address, "100"])
        .status()
        .expect("run child");
    assert!(child.success());
    assert!(parent.wait().expect("wait parent").success());
}
