#![cfg(feature = "urma")]

use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use urma_transport_lab::digest_reader;

const SIXTEEN_MIB: usize = 16 * 1024 * 1024;

struct TestFiles {
    input: PathBuf,
    output: PathBuf,
}

impl TestFiles {
    fn create(label: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let base =
            std::env::temp_dir().join(format!("urma-m4-{label}-{}-{unique}", std::process::id()));
        let files = Self {
            input: base.with_extension("input"),
            output: base.with_extension("output"),
        };
        let mut input = File::create(&files.input).expect("create M4 input");
        let block: Vec<u8> = (0..64 * 1024).map(|index| (index % 251) as u8).collect();
        for _ in 0..SIXTEEN_MIB / block.len() {
            input.write_all(&block).expect("write M4 input");
        }
        files
    }
}

impl Drop for TestFiles {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.input);
        let _ = fs::remove_file(&self.output);
    }
}

fn run_transfer(device: &str, address: &str, input: &Path, output: &Path) {
    let mut parent = Command::new(env!("CARGO_BIN_EXE_parent"))
        .args([device, address])
        .arg(input)
        .spawn()
        .expect("start M4 parent");
    thread::sleep(Duration::from_millis(500));
    let child = Command::new(env!("CARGO_BIN_EXE_child"))
        .args([device, address])
        .arg(output)
        .status()
        .expect("run M4 child");
    assert!(child.success());
    assert!(parent.wait().expect("wait M4 parent").success());

    let (input_digest, input_length) =
        digest_reader(&mut File::open(input).expect("reopen input")).expect("digest input");
    let (output_digest, output_length) =
        digest_reader(&mut File::open(output).expect("open output")).expect("digest output");
    assert_eq!(output_length, input_length);
    assert_eq!(output_digest, input_digest);
}

#[test]
#[ignore = "requires a real URMA provider and hardware"]
fn transfers_sixteen_mib_file() {
    let device = std::env::var("URMA_TEST_DEVICE").unwrap_or_else(|_| "urma0".into());
    let files = TestFiles::create("once");
    run_transfer(&device, "127.0.0.1:31911", &files.input, &files.output);
}

#[test]
#[ignore = "requires a real URMA provider and hardware"]
fn transfers_sixteen_mib_ten_times() {
    let device = std::env::var("URMA_TEST_DEVICE").unwrap_or_else(|_| "urma0".into());
    let files = TestFiles::create("ten-times");
    for iteration in 0..10u16 {
        run_transfer(
            &device,
            &format!("127.0.0.1:{}", 31920 + iteration),
            &files.input,
            &files.output,
        );
    }
}
