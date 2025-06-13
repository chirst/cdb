FROM mcr.microsoft.com/devcontainers/go:1-1.24-bullseye AS base

ADD https://ziglang.org/download/0.14.1/zig-aarch64-linux-0.14.1.tar.xz /usr/bin

RUN tar xf /usr/bin/zig-aarch64-linux-0.14.1.tar.xz -C /usr/bin

ENV PATH="/usr/bin/zig-aarch64-linux-0.14.1:$PATH"
