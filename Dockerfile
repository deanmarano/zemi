# Stage 1: Build static binary using Zig cross-compilation
FROM alpine:3.20 AS builder

# Install Zig (using the official tarball for reproducibility)
ARG ZIG_VERSION=0.14.1
ARG TARGETARCH

RUN apk add --no-cache curl xz && \
    case "${TARGETARCH}" in \
        amd64) ZIG_ARCH="x86_64" ;; \
        arm64) ZIG_ARCH="aarch64" ;; \
        *) echo "Unsupported arch: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz" \
        | tar -xJ -C /usr/local && \
    ln -s /usr/local/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}/zig /usr/local/bin/zig

WORKDIR /build
COPY build.zig build.zig.zon ./
COPY src/ src/

# Build a static, release-optimized binary targeting linux-musl.
# Zig cross-compiles natively — no extra toolchain needed.
ARG TARGETARCH
RUN case "${TARGETARCH}" in \
        amd64) ZIG_TARGET="x86_64-linux-musl" ;; \
        arm64) ZIG_TARGET="aarch64-linux-musl" ;; \
    esac && \
    zig build -Dtarget="${ZIG_TARGET}" -Doptimize=ReleaseSafe && \
    cp zig-out/bin/zemi /zemi && \
    strip /zemi || true

# Stage 2: Minimal runtime image
FROM scratch

COPY --from=builder /zemi /zemi

# Health check port (optional, set HEALTH_PORT to enable)
EXPOSE 4005

ENTRYPOINT ["/zemi"]
