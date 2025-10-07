fn main() {
    // needed for LocalRuntime, can be removed once stabilized
    // see https://github.com/tokio-rs/tokio/issues/7558
    println!("cargo::rustc-cfg=tokio_unstable");
}
