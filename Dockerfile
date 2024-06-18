FROM debian:latest

RUN apt-get update && apt-get install -y curl jq wget xz-utils librocksdb-dev

RUN export ZIG_VERSION=$(curl https://ziglang.org/download/index.json | jq -r '.master.version') && \
    export ZIG="zig-linux-x86_64-$ZIG_VERSION" && \
    export ZIG_DOWNLOAD="https://ziglang.org/builds/$ZIG.tar.xz" && \
    wget $ZIG_DOWNLOAD && tar xf $ZIG.tar.xz && mv $ZIG zig && ln -s /zig/zig /usr/bin

ADD . /src

WORKDIR /src

RUN zig build

FROM debian:latest

RUN apt-get update && apt-get install -y librocksdb7.8

COPY --from=0 /src/zig-out/bin/httpdb /bin/

CMD ["httpdb"]
