FROM rust:1.74 as builder
WORKDIR /usr/src/cse138

# # Start of cache to trick Docker and remember to comment out "Start of comment the below..."
# # Copying the Cargo files separately to cache dependencies
# COPY Cargo.toml Cargo.lock ./

# # Placeholder source file to compile dependencies
# RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release

# # Copy the actual source files and recompile
# # This should be fast if only source files change

# COPY src/ src/
# RUN touch src/main.rs && cargo build --release
# # End of cache to trick Docker

# Start of comment the below out if using start of cache to trick Docker
COPY . . 
RUN cargo build --release
# End of comment the below out if using start of cache to trick Docker

FROM debian:bookworm-slim
COPY --from=builder /usr/src/cse138/target/release/pa3 /usr/local/bin/pa3

# Bypassing signature verification due to known issues with Debian repositories. Very unsafe, but I'm gay.
RUN apt-get -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false update && \
    apt-get install -y libssl-dev && \
    rm -rf /var/lib/apt/lists/*

EXPOSE 8090
CMD ["pa3"]
