cargo build --release
strip -S target/release/dsar
CARGO_APP_VERSION=$(grep ^version Cargo.toml | sed 's/.*"\(.*\)"/\1/')
tar czvf dsar-osx-intel-v${CARGO_APP_VERSION}-1.tar.gz -C target/release dsar
