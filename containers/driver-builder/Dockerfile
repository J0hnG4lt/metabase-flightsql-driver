# JDK 21 is the build baseline. Metabase 63/64 runtime images use JDK 25
# with the Arrow JVM flags configured in compose.
FROM eclipse-temurin:21-jdk-jammy

# Install curl for downloading Leiningen
RUN apt-get update && apt-get install -y curl

# Install Leiningen
RUN curl -O https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein \
    && mv lein /usr/local/bin/lein \
    && chmod +x /usr/local/bin/lein

WORKDIR /builder


