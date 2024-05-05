#[rustversion::nightly]
fn main() {
    println!("cargo:rustc-check-cfg=cfg(nightly)");
    println!("cargo:rustc-cfg=nightly");
}

#[rustversion::not(nightly)]
fn main() {
    println!("cargo:rustc-check-cfg=cfg(nightly)");
}
