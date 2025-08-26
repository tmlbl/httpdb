FROM debian:latest

ARG ARCH=x86_64

RUN apt-get update && apt-get install -y curl jq wget xz-utils librocksdb-dev

RUN export ZIG_VERSION=0.15.1 && \
    export ZIG="zig-${ARCH}-linux-$ZIG_VERSION" && \
    export ZIG_DOWNLOAD="https://ziglang.org/download/$ZIG_VERSION/$ZIG.tar.xz" && \
    wget $ZIG_DOWNLOAD && tar xf $ZIG.tar.xz && mv $ZIG zig && ln -s /zig/zig /usr/bin

ADD . /src

WORKDIR /src

RUN zig build -Doptimize=ReleaseFast

FROM debian:latest

RUN apt-get update && apt-get install -y librocksdb9.10

COPY --from=0 /src/zig-out/bin/httpdb /bin/

CMD ["httpdb"]
