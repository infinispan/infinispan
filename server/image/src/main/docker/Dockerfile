## Arguments
# The name of the OpenJDK package (e.g. java-25-openjdk-headless) in case it should be installed from the distribution repository.
# If set, JDK_DIST below will be ignored.
ARG OPENJDK_PACKAGE=""
# Packages needed by the builder
ARG BUILDER_PACKAGES="file unzip wget"
# Packages to be installed in the target image
ARG MAIN_PACKAGES="coreutils-single curl grep iproute libstdc++ lsof procps-ng sed tar vim-minimal ${OPENJDK_PACKAGE}"
# Packages to be removed
ARG REMOVE_PACKAGES="alsa-lib avahi-libs cups-filesystem cups-libs gawk p11-kit"
# The path to the root filesystem in the builder
ARG NEWROOT=/new-root-fs
# The path where DNF cache files should be stored
ARG DNF_CACHE=/tmp/dnf-cache
# Replace "--releasever 10" below with "--use-host-config" once DNF is upgraded
ARG DNF_FLAGS="-y --installroot=${NEWROOT} \
      --releasever 10 \
      --nodocs \
      --setopt=install_weak_deps=False \
      --setopt=cachedir=${DNF_CACHE} \
      --setopt=system_cachedir=${DNF_CACHE} \
      --setopt=logdir=${DNF_CACHE} \
      --setopt=varsdir=${DNF_CACHE}"
# The JDK distribution. Can be a local file or a remote URL. Supports zip, tar.gz and rpm.
# If it points to an empty file, the JDK will be downloaded from Adoptium.
# If OPENJDK_PACKAGE is non-empty, then that will be used instead.
ARG JDK_DIST=src/main/docker/adoptium
# The Infinispan server distribution. Can be a local file or a remote URL. Must be a zip.
ARG SERVER_DIST
# The target architecture. This is automatically configured by the container builder.
ARG TARGETARCH
# The path where the Infinispan server will be installed
ARG WORKDIR=/opt/infinispan
# Brand name
ARG BRAND_NAME=Infinispan
# Brand version
ARG BRAND_VERSION
# The base image
ARG BASE_IMAGE=scratch

# Builder
FROM registry.access.redhat.com/ubi10 AS builder
# Repeat the arguments, since they have been reset by the FROM directive above
ARG NEWROOT
ARG DNF_CACHE
ARG DNF_FLAGS
ARG JDK_DIST
ARG SERVER_DIST
ARG BUILDER_PACKAGES
ARG OPENJDK_PACKAGE
ARG MAIN_PACKAGES
ARG REMOVE_PACKAGES
ARG TARGETARCH
ARG WORKDIR

# Add the JDK distribution
ADD $JDK_DIST /tmp/jdk

# Add the Infinispan server distribution
ADD $SERVER_DIST /tmp/infinispan

# Add builder packages
RUN dnf install -y --nodocs ${BUILDER_PACKAGES}

# Add target packages
RUN mkdir -p ${NEWROOT} ${DNF_CACHE}
RUN dnf ${DNF_FLAGS} install ${MAIN_PACKAGES}

# Install the JDK distribution
ADD src/main/docker/install-jdk.sh /tmp/
RUN /tmp/install-jdk.sh ${NEWROOT} /tmp/jdk ${OPENJDK_PACKAGE}

# Remove unwanted packages. Ignore errors
RUN rpm -r ${NEWROOT} --erase --nodeps --allmatches ${REMOVE_PACKAGES} || true

# Delete unnecessary files
RUN rm -rf \
      ${NEWROOT}/usr/lib/sysimage \
      ${NEWROOT}/usr/share/licenses \
      ${NEWROOT}/usr/share/locale \
      ${NEWROOT}/var/lib/dnf \
      ${NEWROOT}${DNF_CACHE}

# Install the Infinispan server distribution
ADD src/main/docker/install-server.sh /tmp/
RUN /tmp/install-server.sh ${NEWROOT} ${WORKDIR}
ADD src/main/docker/launch.sh ${NEWROOT}${WORKDIR}/bin/
# Build the AOT cache using a fixed memory configuration. This will work for heaps < 32GB
ENV JAVA_HOME=${NEWROOT}/usr/lib/jvm/default-java
ENV JAVA_OPTS="-XX:+ExitOnOutOfMemoryError -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -Xms64m -Xmx512m"
# Temporarily disabled
RUN ${NEWROOT}/${WORKDIR}/bin/server.sh --aot || true
ADD src/main/docker/bashrc ${NEWROOT}/${WORKDIR}/.bashrc
RUN chown -R 65532:0 ${NEWROOT}/${WORKDIR}
# Backwards compatibility
RUN mkdir -p ${NEWROOT}/opt/gossiprouter/bin
RUN ln -s -r ${NEWROOT}/${WORKDIR}/bin/gossip-router.sh ${NEWROOT}/opt/gossiprouter/bin/launch.sh
RUN chown -R 65532:0 ${NEWROOT}/opt/gossiprouter

# Target image
FROM ${BASE_IMAGE}
ARG NEWROOT
ARG WORKDIR
ARG BRAND_NAME
ARG BRAND_VERSION

# Copy from the builder image to the target
COPY --from=builder ${NEWROOT} /

# Add the user and group
RUN echo "infinispan:x:0:root" >> /etc/group && \
    echo "infinispan:x:65532:0:infinispan:/opt/infinispan:/usr/sbin/nologin" >> /etc/passwd

# We run the container unprivileged, create a writable HOME
WORKDIR ${WORKDIR}
ENV HOME=${WORKDIR}
ENV ISPN_HOME=/opt/infinispan
ENV USER=infinispan
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV LANG=C.UTF-8
USER 65532
EXPOSE 11222 11221 11223 2157 46655 57800 7800 7900 8080 8443
ENTRYPOINT [ "/opt/infinispan/bin/launch.sh" ]

# Labels
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
