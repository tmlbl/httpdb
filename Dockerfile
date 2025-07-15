FROM debian:latest

ENV ARCH=aarch64

RUN apt-get update && apt-get install -y curl jq wget xz-utils librocksdb-dev

RUN export ZIG_VERSION=0.14.0 && \
    export ZIG="zig-linux-${ARCH}-$ZIG_VERSION" && \
    export ZIG_DOWNLOAD="https://ziglang.org/download/$ZIG_VERSION/$ZIG.tar.xz" && \
    wget $ZIG_DOWNLOAD && tar xf $ZIG.tar.xz && mv $ZIG zig && ln -s /zig/zig /usr/bin

ADD . /src

WORKDIR /src

RUN zig build

FROM debian:latest

RUN apt-get update && apt-get install -y librocksdb7.8

COPY --from=0 /src/zig-out/bin/httpdb /bin/

CMD ["httpdb"]
