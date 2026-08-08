use urma_transport_lab::{abi_baseline, RuntimeConfig, UrmaRuntime};

fn main() {
    let device = std::env::args().nth(1).unwrap_or_else(|| "urma0".into());
    let config = RuntimeConfig::new(device, 0);
    if let Ok(abi) = abi_baseline() {
        println!("child: ABI baseline {abi:?}");
    }
    match UrmaRuntime::open(config) {
        Ok(runtime) => {
            println!("child: liburma device/context ready; Jetty/OOB/data path are Phase 0 TODOs");
            if let Err(error) = runtime.close() {
                eprintln!("child: shutdown failed: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("child: runtime unavailable: {error}");
            eprintln!("build on Linux with --features urma and pass the device name as argument");
        }
    }
}
