# Compiling on Mac OSX

For compiling Rust OSX packages, it's easy to compile an execute for both x86_64 and aarch64 (AMD).
Depending on the architecture, the rust installation via cargo will have installed the build target for that architecture:
- aarch64: aarch64-apple-darwin
- x86_64: x86_64-apple-darwin

If you want to create an executable on aarch64 for x86_64, or vice versa, all you need to is:
- install the build target for the other architecture.
```shell
rustup target add x86_64-apple-darwin # for adding intel on ARM
rustup target add aarch64-apple-darwin # for adding ARM on intel
```
- specify the target architecture with cargo build.
```shell
cargo build --target=x86_64-apple-darwin # for building an intel executable on ARM
cargo build --target=aarch64-apple-darwin # for building an ARM execute on intel
```

Please mind that if you specify a `--target`, the built execute will be placed in target/<build-target>/<release|debug>/<executable>

# Colima
In order to use docker to build x86_64 executables on aarch64, use colima as docker target.
Colima can be installing using brew: 
```shell
brew install colima
```
After that is done, you must setup a virtual machine that can run the different architecture:
```shell
colima start --arch x86_64 
```
This will setup a qemu based VM with 2 CPUs, 2GiB memory and 60GiB storage, with a docker runtime.

On my Mac M1, I like to increase the CPUs and memory, and change the virtualization to Mac based with rosetta 2:
```shell
colima delete
colima start --arch x86_64 --cpu 6 --memory 8 --vm-type=vz --vz-rosetta
```

# Docker
To use docker, install docker with brew:
```shell
brew install docker
```
However, to use docker for building executables, and be able to use `docker build --output`, install docker-buildx:
```shell
brew install docker-buildx
```
Caveat is that there is a post install requirement before docker can pick up buildx:
```shell
mkdir -p ~/.docker/cli-plugins
ln -sfn /opt/homebrew/opt/docker-buildx/bin/docker-buildx ~/.docker/cli-plugins/docker-buildx
```
Docker will automatically use the colima VM, and thus would allow to run images in x86_64 on aarch64 (on Mac Mini M1).