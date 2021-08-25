fn main() {
    //tonic_build::configure()
    //    .type_attribute("routeguide.Point", "#[derivce(Hash)]")
    //    .compile()
    //    .unwrap();

    //tonic_build::compile_protos("proto/bcl.proto").unwrap();
    //tonic_build::compile_protos("proto/MyServiceBusQueuePersistenceGrpcService.proto").unwrap();
    //tonic_build::compile_protos("proto/MyServiceBusHistoryReaderGrpcService.proto").unwrap();
    tonic_build::compile_protos("proto/MyServicePersistenceGrpcService.proto").unwrap();
}
