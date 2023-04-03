fn main() {
    let url = "https://raw.githubusercontent.com/MyJetTools/my-sb-proto-files/main/proto/";
    ci_utils::sync_and_build_proto_file(url, "MyServicePersistenceGrpcService.proto");
}
