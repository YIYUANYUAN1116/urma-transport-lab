#![cfg(feature = "urma")]

use std::{process::Command, thread, time::Duration};

/// Requires a real provider and validates 64-bit SEND_IMM values over the
/// same RC duplex Jetty/shared-JFR foundation used by the file-transfer demo.
#[test]
#[ignore = "requires a real URMA provider and hardware"]
fn rc_send_imm_preserves_identity_and_payload_binding() {
    let device = std::env::var("URMA_TEST_DEVICE").unwrap_or_else(|_| "urma0".into());
    let eid_index = std::env::var("URMA_TEST_EID_INDEX").unwrap_or_else(|_| "0".into());
    let address = "127.0.0.1:31911";
    let mut parent = Command::new(env!("CARGO_BIN_EXE_send_imm_probe"))
        .args(["parent", &device, &eid_index, address, "64"])
        .spawn()
        .expect("start SEND_IMM parent");
    thread::sleep(Duration::from_millis(500));
    let child = Command::new(env!("CARGO_BIN_EXE_send_imm_probe"))
        .args(["child", &device, &eid_index, address, "64"])
        .status()
        .expect("run SEND_IMM child");
    assert!(child.success());
    assert!(parent.wait().expect("wait SEND_IMM parent").success());
}
