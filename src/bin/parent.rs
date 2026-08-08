use urma_transport_lab::{abi_baseline, RuntimeConfig, UrmaRuntime};

fn main() {
    run("parent");
}

fn run(role: &str) {
    let device = std::env::args().nth(1).unwrap_or_else(|| "urma0".into());
    let config = RuntimeConfig::new(device, 0);
    if let Ok(abi) = abi_baseline() {
        println!("{role}: ABI baseline {abi:?}");
    }
    match UrmaRuntime::open(config) {
        Ok(runtime) => {
            println!("{role}: M1 capability: {:?}", runtime.capability());
            println!(
                "{role}: JFC depths {:?}, registered memory {:?}",
                runtime.jfc_depths(),
                runtime.registered_memory_layout()
            );
            println!("{role}: M1 ready; Jetty/OOB/data path remain TODOs");
            if let Err(error) = runtime.close() {
                eprintln!("{role}: shutdown failed: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("{role}: runtime unavailable: {error}");
            eprintln!("build on Linux with --features urma and pass the device name as argument");
        }
    }
}
