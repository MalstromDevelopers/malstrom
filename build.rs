fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("src/network_exchange/service.proto")?;
    Ok(())
}
