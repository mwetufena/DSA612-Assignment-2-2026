# --- builder ---
FROM ballerina/ballerina:2201.13.5 AS builder
WORKDIR /home/ballerina
COPY Ballerina.toml ./
COPY *.bal ./
RUN bal build --optimize-dependency-compilation

# --- runtime ---
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
RUN apk add --no-cache wget
COPY --from=builder /home/ballerina/target/bin/*.jar /app/service.jar
ENV SERVICE_PORT=8090
EXPOSE 8090
ENTRYPOINT ["java", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/service.jar"]