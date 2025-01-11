use std::error::Error;
//use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let out_dir = Path::new("src/proto");
    tonic_build::configure().out_dir(out_dir).compile_protos(
        &["proto/DanubeApi.proto", "proto/DanubeAdmin.proto"],
        &["proto"],
    )?;

    Ok(())
}
