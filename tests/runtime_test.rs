use urma_transport_lab::{Error, RuntimeConfig, UrmaRuntime};

#[cfg(not(feature = "urma"))]
#[test]
fn feature_off_does_not_require_umdk_runtime() {
    let result = UrmaRuntime::start(RuntimeConfig::new("no-umdk-required", 0));
    assert!(matches!(result, Err(Error::FeatureDisabled)));
}

#[cfg(feature = "urma")]
#[test]
fn creation_failure_rolls_back_process_guard() {
    let config = RuntimeConfig::new("__urma_lab_intentionally_missing_device__", 0);
    assert!(UrmaRuntime::start(config.clone()).is_err());
    let second = UrmaRuntime::start(config).err();
    assert!(!matches!(second, Some(Error::AlreadyInitialized)));
}

#[cfg(feature = "urma")]
#[test]
#[ignore = "requires a real Linux UMDK provider/device; set URMA_TEST_DEVICE"]
fn feature_on_enters_complete_m1_initialization_path() {
    let device = std::env::var("URMA_TEST_DEVICE").expect("set URMA_TEST_DEVICE");
    let runtime = UrmaRuntime::start(RuntimeConfig::new(device, 0))
        .expect("M1 resource tree must start on the configured provider");
    assert_eq!(runtime.jfc_depths(), (64, 64));
    assert!(runtime.registered_memory_layout().is_some());
    runtime
        .shutdown()
        .expect("M1 resource tree must close cleanly");
}
