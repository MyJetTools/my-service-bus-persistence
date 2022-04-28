fn main() {
    tonic_build::compile_protos("proto/MyServicePersistenceGrpcService.proto").unwrap();
}
