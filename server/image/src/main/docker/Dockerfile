FROM registry.redhat.io/ubi10 AS ubi-micro-build

# Arguments
ARG BRAND_VERSION
ARG BRAND_NAME
ARG JDK_DIST
ARG SERVER_DIST

# Copy the server distribution
ADD $SERVER_DIST /tmp/infinispan

# Add the JDK distribution
ADD $JDK_DIST /tmp/jdk

ADD src/main/docker/build.sh /tmp/
RUN bash /tmp/build.sh glibc-langpack-en findutils grep sed iproute lsof tzdata-java vim-minimal

FROM registry.redhat.io/ubi10-micro
ENV LANG=en_US.UTF-8

COPY --from=ubi-micro-build /tmp/null/rootfs/ /
COPY --from=ubi-micro-build --chown=185:0 /opt/infinispan /opt/infinispan
ADD src/main/docker/launch.sh /opt/infinispan/bin/

RUN echo "infinispan:x:0:root" >> /etc/group && \
    echo "infinispan:x:185:0:infinispan user:/opt/infinispan:/sbin/nologin" >> /etc/passwd

USER 185

# Build the AOT cache
RUN /opt/infinispan/bin/server.sh --aot || true

ENV ISPN_HOME=/opt/infinispan
ENV USER=default

EXPOSE 11222 11221 11223 2157 46655 57800 7800 7900 8080 8443

ENTRYPOINT [ "/opt/infinispan/bin/launch.sh" ]

# common labels
ARG URL="https://infinispan.org/"
ARG SERVER_DESCRIPTION="Provides a scalable in-memory distributed database designed for fast access to large volumes of data."
ARG SERVER_DISPLAY_NAME="${BRAND_NAME} Server"
ARG TAGS="infinispan datagrid cache"
ARG MAINTAINER=${URL}
ARG VENDOR=${MAINTAINER}

LABEL maintainer=${MAINTAINER} \
      vendor=${VENDOR} \
      version=${BRAND_VERSION} \
      url=${URL} \
      io.openshift.tags=${TAGS} \
      release="" \
      vcs-ref="" \
      com.redhat.build-host="" \
      com.redhat.component="" \
      com.redhat.license_terms=""

# server specific
ARG SERVER_IMAGE_NAME="infinispan"
ARG SERVER_DESCRIPTION="${SERVER_DISPLAY_NAME} Image"

LABEL name=${SERVER_IMAGE_NAME} \
      description=${SERVER_DESCRIPTION} \
      summary=${SERVER_DESCRIPTION} \
      io.k8s.display-name=${SERVER_DISPLAY_NAME} \
      io.k8s.description=${SERVER_DESCRIPTION}

# oci
ARG SOURCE="https://github.com/infinispan/infinispan"
ARG DOCS=${URL}documentation

LABEL org.opencontainers.image.title=${SERVER_DISPLAY_NAME} \
      org.opencontainers.image.url=${URL} \
      org.opencontainers.image.source=${SOURCE} \
      org.opencontainers.image.description=${SERVER_DESCRIPTION} \
      org.opencontainers.image.documentation=${DOCS}
