use std::env;

#[cfg(feature = "urma")]
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

const TRACKED_ENV: &[&str] = &[
    "UMDK_INCLUDE_DIR",
    "UMDK_LIB_DIR",
    "UMDK_PROVIDER_DIR",
    "BINDGEN_EXTRA_CLANG_ARGS",
];

fn main() {
    for name in TRACKED_ENV {
        println!("cargo:rerun-if-env-changed={name}");
    }
    for source in [
        "src/ffi/wrapper.h",
        "src/ffi/shim.h",
        "src/ffi/shim.c",
    ] {
        println!("cargo:rerun-if-changed={source}");
    }

    // This early return is the central feature-isolation guarantee. In a
    // feature-off build no UMDK path is inspected and no native tool is run.
    if env::var_os("CARGO_FEATURE_URMA").is_none() {
        return;
    }

    let target_os = required_env("CARGO_CFG_TARGET_OS");
    if target_os != "linux" {
        panic!("feature `urma` requires a Linux target; Cargo selected `{target_os}`");
    }

    build_urma();
}

#[cfg(feature = "urma")]
fn build_urma() {
    let include_dir = find_include_dir();
    let lib_dir = find_lib_dir();
    let api_header = include_dir.join("urma_api.h");

    track_public_headers(&include_dir);
    println!("cargo:warning=URMA M0 include: {}", include_dir.display());
    println!("cargo:warning=URMA M0 library: {}", lib_dir.display());
    if let Some(provider_dir) = env::var_os("UMDK_PROVIDER_DIR") {
        println!(
            "cargo:warning=URMA provider directory (runtime only): {}",
            PathBuf::from(provider_dir).display()
        );
    }

    // Bindgen parses the installed public header through wrapper.h. Only the
    // verified M0 liburma calls and the stable lab shim are emitted.
    let mut bindings = bindgen::Builder::default()
        .header("src/ffi/wrapper.h")
        .clang_arg(format!("-I{}", include_dir.display()))
        .allowlist_function(
            "^urma_(init|uninit|get_device_by_name|create_context|delete_context)$",
        )
        .allowlist_function("^urma_lab_.*")
        .allowlist_type("^urma_(init_attr|device|context|lab_.*)(_t)?$")
        .allowlist_var("^URMA_(SUCCESS|LAB_.*)$")
        .opaque_type("^urma_(device|context)$")
        .derive_debug(false)
        .derive_default(false)
        .layout_tests(true)
        .generate_comments(true);

    if let Ok(extra) = env::var("BINDGEN_EXTRA_CLANG_ARGS") {
        for arg in extra.split_whitespace() {
            bindings = bindings.clang_arg(arg);
        }
    }

    let generated = bindings.generate().unwrap_or_else(|error| {
        panic!(
            "failed to generate bindings from {}: {error}",
            api_header.display()
        )
    });
    let out_dir = PathBuf::from(required_env_os("OUT_DIR"));
    generated
        .write_to_file(out_dir.join("urma_bindings.rs"))
        .expect("failed to write generated URMA bindings into OUT_DIR");

    cc::Build::new()
        .file("src/ffi/shim.c")
        .include(&include_dir)
        .warnings(true)
        .extra_warnings(true)
        .compile("urma_lab_shim");

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=urma");
}

#[cfg(feature = "urma")]
fn find_include_dir() -> PathBuf {
    if let Some(configured) = env::var_os("UMDK_INCLUDE_DIR") {
        let configured = PathBuf::from(configured);
        let candidates = [configured.clone(), configured.join("ub/umdk/urma")];
        return find_file_parent(&candidates, "urma_api.h").unwrap_or_else(|| {
            panic!(
                "UMDK_INCLUDE_DIR={} contains neither urma_api.h nor ub/umdk/urma/urma_api.h",
                configured.display()
            )
        });
    }

    let candidates = [
        PathBuf::from("/usr/include/ub/umdk/urma"),
        PathBuf::from("/usr/local/include/ub/umdk/urma"),
    ];
    find_file_parent(&candidates, "urma_api.h").unwrap_or_else(|| {
        panic!(
            "cannot find urma_api.h; install UMDK development headers or set UMDK_INCLUDE_DIR"
        )
    })
}

#[cfg(feature = "urma")]
fn find_lib_dir() -> PathBuf {
    if let Some(configured) = env::var_os("UMDK_LIB_DIR") {
        let configured = PathBuf::from(configured);
        return find_file_parent(std::slice::from_ref(&configured), "liburma.so")
            .unwrap_or_else(|| {
                panic!(
                    "UMDK_LIB_DIR={} does not contain the linker input liburma.so",
                    configured.display()
                )
            });
    }

    let arch = required_env("CARGO_CFG_TARGET_ARCH");
    let multiarch = match arch.as_str() {
        "aarch64" => Some("aarch64-linux-gnu"),
        "x86_64" => Some("x86_64-linux-gnu"),
        _ => None,
    };
    let mut candidates = vec![
        PathBuf::from("/usr/lib64"),
        PathBuf::from("/usr/lib"),
        PathBuf::from("/usr/local/lib64"),
        PathBuf::from("/usr/local/lib"),
    ];
    if let Some(multiarch) = multiarch {
        candidates.push(PathBuf::from("/usr/lib").join(multiarch));
        candidates.push(PathBuf::from("/usr/local/lib").join(multiarch));
    }

    find_file_parent(&candidates, "liburma.so").unwrap_or_else(|| {
        panic!("cannot find linker input liburma.so; install UMDK development libraries or set UMDK_LIB_DIR")
    })
}

#[cfg(feature = "urma")]
fn find_file_parent(candidates: &[PathBuf], file: &str) -> Option<PathBuf> {
    candidates
        .iter()
        .find(|candidate| candidate.join(file).is_file())
        .cloned()
}

#[cfg(feature = "urma")]
fn track_public_headers(include_dir: &Path) {
    let Ok(entries) = include_dir.read_dir() else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|extension| extension == "h") {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("Cargo must set {name}"))
}

#[cfg(feature = "urma")]
fn required_env_os(name: &str) -> OsString {
    env::var_os(name).unwrap_or_else(|| panic!("Cargo must set {name}"))
}

#[cfg(not(feature = "urma"))]
fn build_urma() {
    unreachable!("build_urma is called only when the urma feature is enabled")
}
