use std::error::Error;
//use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let out_dir = Path::new("src/proto");
    tonic_prost_build::configure()
        .out_dir(out_dir)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                "proto/DanubeApi.proto",
                "proto/DanubeAdmin.proto",
                "proto/SchemaRegistry.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
