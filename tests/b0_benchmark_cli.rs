use std::process::Command;

#[test]
fn dry_run_prints_one_validated_case_json_line() {
    let output = Command::new(env!("CARGO_BIN_EXE_benchmark"))
        .args([
            "--dry-run",
            "--case-id",
            "cli-smoke",
            "--scenario",
            "file",
            "--transport",
            "tcp-sendfile",
            "--bytes",
            "1048576",
            "--chunk-size",
            "262144",
            "--window",
            "1",
            "--timing-mode",
            "setup-included",
            "--completion-policy",
            "durable",
            "--seed",
            "42",
        ])
        .output()
        .expect("run benchmark dry-run");
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "{\"case_id\":\"cli-smoke\",\"repeat\":1,\"scenario\":\"file\",\"transport\":\"tcp-sendfile\",\"bytes\":1048576,\"chunk_size\":262144,\"window\":1,\"timing_mode\":\"setup-included\",\"completion_policy\":\"durable\",\"data_seed\":42}\n"
    );
}

#[test]
fn dry_run_rejects_transport_scenario_mismatch() {
    let output = Command::new(env!("CARGO_BIN_EXE_benchmark"))
        .args([
            "--dry-run",
            "--scenario",
            "memory",
            "--transport",
            "tcp-sendfile",
        ])
        .output()
        .expect("run invalid benchmark dry-run");
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("tcp-sendfile is only valid for the file scenario"));
}

#[test]
fn help_documents_reproducible_output_creation() {
    let output = Command::new(env!("CARGO_BIN_EXE_benchmark"))
        .arg("--help")
        .output()
        .expect("run benchmark help");
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("--output-mode fresh|truncate"));
    assert!(stdout.contains("default: fresh"));
    assert!(stdout.contains("--cleanup-output"));
    assert!(stdout.contains("default: file=4"));
}
