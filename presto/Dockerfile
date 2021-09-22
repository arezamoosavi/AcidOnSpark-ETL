FROM openjdk:8-jre

# Update the base image OS and install wget and python
RUN apt-get update && apt-get install -y wget python less

# Presto version will be passed in at build time
ARG PRESTO_VERSION=0.218

# Set the URL to download
ARG PRESTO_PKG=presto-server-${PRESTO_VERSION}.tar.gz
ARG PRESTO_URL=https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz

ARG PRESTO_CLI=presto-cli-${PRESTO_VERSION}-executable.jar
ARG PRESTO_CLI_URL=https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar


# Download Presto and unpack it to /opt/presto
RUN curl -o /${PRESTO_PKG} ${PRESTO_URL} && \
    mkdir -p /opt && \
    tar -xf ${PRESTO_PKG} -C /opt && \
    rm ${PRESTO_PKG} && \
    ln -s /opt/presto-server-${PRESTO_VERSION} /opt/presto

# Download the Presto CLI and put it in the image
RUN curl -o /${PRESTO_CLI} ${PRESTO_CLI_URL} && \
    mv ${PRESTO_CLI} /usr/local/bin/presto && \
    chmod +x /usr/local/bin/presto

# Copy configuration files on the host into the image
COPY etc /opt/presto/etc

# Specify the entrypoint to start
ENTRYPOINT /opt/presto/bin/launcher run
EXPOSE 8888