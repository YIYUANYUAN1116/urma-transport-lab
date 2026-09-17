#![cfg(feature = "urma")]

use std::{process::Command, thread, time::Duration};

/// Runs two local processes over one EID. This exercises a real RM/RTP READ,
/// raw CQE routing, owner retirement, content integrity, and ordered teardown.
#[test]
#[ignore = "requires a real URMA provider and hardware"]
fn rm_read_loopback_routes_cqes_and_retires_owners() {
    let device = std::env::var("URMA_TEST_DEVICE").unwrap_or_else(|_| "urma0".into());
    let eid_index = std::env::var("URMA_TEST_EID_INDEX").unwrap_or_else(|_| "0".into());
    let address = "127.0.0.1:31912";
    let mut parent = Command::new(env!("CARGO_BIN_EXE_rm_read_probe"))
        .args([
            "parent", &device, &eid_index, address, "67108864", "1048576", "128",
        ])
        .spawn()
        .expect("start RM READ parent");
    thread::sleep(Duration::from_millis(500));
    let child = Command::new(env!("CARGO_BIN_EXE_rm_read_probe"))
        .args([
            "child", &device, &eid_index, address, "67108864", "1048576", "128",
        ])
        .status()
        .expect("run RM READ child");
    assert!(child.success());
    assert!(parent.wait().expect("wait RM READ parent").success());
}
