# build OSX packages for x86_64 as well as aarch64
#
# App version from cargo.toml
CARGO_APP_VERSION=$(grep ^version Cargo.toml | sed 's/.*"\(.*\)"/\1/')
# x86_64
rustup target add x86_64-apple-darwin
cargo build --release --target=x86_64-apple-darwin
strip -S target/x86_64-apple-darwin/release/dsar
tar czvf dsar-osx-x86_64-v${CARGO_APP_VERSION}-1.tar.gz -C target/x86_64-apple-darwin/release dsar
# aarch64
rustup target add aarch64-apple-darwin
cargo build --release --target=aarch64-apple-darwin
strip -S target/aarch64-apple-darwin/release/dsar
tar czvf dsar-osx-aarch64-v${CARGO_APP_VERSION}-1.tar.gz -C target/aarch64-apple-darwin/release dsar
