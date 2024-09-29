use std::{env, path::PathBuf};

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("talaria_descriptor.bin"))
        .out_dir("./src")
        .compile(&["./protos/talaria_rs.proto"], &["protos"])
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));
}
