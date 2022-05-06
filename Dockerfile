FROM rust:slim
COPY ./wwwroot ./wwwroot 
COPY ./target/release/my-sb-persistence ./target/release/my-sb-persistence 
ENTRYPOINT ["./target/release/my-sb-persistence"]