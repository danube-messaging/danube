use std::error::Error;
//use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let out_dir = Path::new("src/proto");
    tonic_build::configure().out_dir(out_dir).compile(
        &["../proto/DanubeApi.proto", "../proto/DanubeAdmin.proto"],
        &["../proto"],
    )?;

    // let client_proto_dir = Path::new("../danube-client/src/proto");
    // let admin_proto_dir = Path::new("../danube-admin-cli/src/proto");

    // // copy the generated api file to danube-client crate
    // fs::copy(
    //     out_dir.join("danube.rs"),
    //     client_proto_dir.join("danube.rs"),
    // )?;

    // // copy the generated admin file to danube-admin-cli crate
    // fs::copy(
    //     out_dir.join("danube_admin.rs"),
    //     admin_proto_dir.join("danube_admin.rs"),
    // )?;

    Ok(())
}
