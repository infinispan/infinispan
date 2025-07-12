FROM registry.redhat.io/ubi10 AS ubi-micro-build

ARG INFINISPAN_VERSION
ARG FEDORA_REPO=https://dl.fedoraproject.org/pub/fedora/linux/development/rawhide/Everything/x86_64/os/Packages/j/
ARG JDK_RPM=java-25-openjdk-headless-25.0.0.0.26-0.3.ea.fc43.x86_64.rpm

ADD target/infinispan-server-$INFINISPAN_VERSION /opt/infinispan/
RUN chmod -R g+rwX /opt/infinispan

ADD $FEDORA_REPO/$JDK_RPM /tmp/
# Comment above and uncomment below to use local copy
#ADD $JDK_RPM /tmp/

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

ENV ISPN_HOME=/opt/infinispan
ENV USER=default

EXPOSE 11222 11221 11223 2157 46655 57800 7800 7900 8080 8443

ENTRYPOINT [ "/opt/infinispan/bin/launch.sh" ]

# common labels
ARG INFINISPAN_URL="https://infinispan.org/"
ARG INFINISPAN_SERVER_DESCRIPTION="Provides a scalable in-memory distributed database designed for fast access to large volumes of data."
ARG INFINISPAN_SERVER_DISPLAY_NAME="Infinispan Server"
ARG INFINISPAN_TAGS="infinispan datagrid cache"
ARG INFINISPAN_MAINTAINER=${INFINISPAN_URL}
ARG INFINISPAN_VENDOR=${INFINISPAN_MAINTAINER}

LABEL maintainer=${INFINISPAN_MAINTAINER} \
      vendor=${INFINISPAN_VENDOR} \
      version=${INFINISPAN_VERSION} \
      url=${INFINISPAN_URL} \
      io.openshift.tags=${INFINISPAN_TAGS} \
      release="" \
      vcs-ref="" \
      com.redhat.build-host="" \
      com.redhat.component="" \
      com.redhat.license_terms=""

# server specific
ARG INFINISPAN_SERVER_DISPLAY_NAME="Infinispan Server"
ARG INFINISPAN_SERVER_IMAGE_NAME="infinispan"
ARG INFINISPAN_SERVER_DESCRIPTION="${INFINISPAN_SERVER_DISPLAY_NAME} Image"

LABEL name=${INFINISPAN_SERVER_IMAGE_NAME} \
      description=${INFINISPAN_SERVER_DESCRIPTION} \
      summary=${INFINISPAN_SERVER_DESCRIPTION} \
      io.k8s.display-name=${INFINISPAN_SERVER_DISPLAY_NAME} \
      io.k8s.description=${INFINISPAN_SERVER_DESCRIPTION}

# oci
ARG INFINISPAN_SOURCE="https://github.com/infinispan/infinispan"
ARG INFINISPAN_DOCS=${INFINISPAN_URL}documentation

LABEL org.opencontainers.image.title=${INFINISPAN_SERVER_DISPLAY_NAME} \
      org.opencontainers.image.url=${INFINISPAN_URL} \
      org.opencontainers.image.source=${INFINISPAN_SOURCE} \
      org.opencontainers.image.description=${INFINISPAN_SERVER_DESCRIPTION} \
      org.opencontainers.image.documentation=${INFINISPAN_DOCS}