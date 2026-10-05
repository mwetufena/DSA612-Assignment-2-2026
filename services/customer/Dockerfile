# --- builder ---
FROM ballerina/ballerina:2201.13.5 AS builder
WORKDIR /home/ballerina

# Cache dependencies first
COPY Ballerina.toml ./
RUN bal version || true

# Copy source and build
COPY *.bal ./
RUN bal build --optimize-dependency-compilation

# --- runtime ---
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Health probe deps
RUN apk add --no-cache wget

COPY --from=builder /home/ballerina/target/bin/*.jar /app/service.jar

ENV SERVICE_PORT=8090
EXPOSE 8090

# Graceful shutdown on SIGTERM, generous startup time
ENTRYPOINT ["java", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/service.jar"]