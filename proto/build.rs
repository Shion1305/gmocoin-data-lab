fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = ["market.proto", "backtest.proto"];
    let include_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .extern_path(".google.protobuf.Timestamp", "::prost_types::Timestamp")
        .compile(
            &protos
                .iter()
                .map(|name| include_dir.join(name))
                .collect::<Vec<_>>(),
            &[include_dir.to_path_buf()],
        )?;

    println!("cargo:rerun-if-changed=build.rs");
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    Ok(())
}
