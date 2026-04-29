use std::error::Error;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let out_dir = Path::new("src/proto");
    std::fs::create_dir_all(out_dir)?;

    tonic_prost_build::configure()
        .out_dir(out_dir)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/EdgeReplicator.proto"], &["proto"])?;

    Ok(())
}
