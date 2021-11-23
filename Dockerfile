FROM rust:slim
COPY ./target/release/my-sb-persistence ./target/release/my-sb-persistence 
ENTRYPOINT ["./target/release/my-sb-persistence"]