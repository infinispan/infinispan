FROM jboss/base-jdk:11

# Set the INFINISPAN_SERVER_HOME env variable
ENV INFINISPAN_SERVER_HOME /opt/jboss/infinispan-server

# Set the INFINISPAN_VERSION env variable
ENV INFINISPAN_VERSION ${}

# Ensure signals are forwarded to the JVM process correctly for graceful shutdown
ENV LAUNCH_JBOSS_IN_BACKGROUND true

# Server download location
ENV DISTRIBUTION_URL https://downloads.jboss.org/infinispan/$INFINISPAN_VERSION/infinispan-server-$INFINISPAN_VERSION.zip

# Labels
LABEL name="Infinispan Server" \
      version="$INFINISPAN_VERSION" \
      release="$INFINISPAN_VERSION" \
      architecture="x86_64" \
      io.k8s.description="Provides a scalable in-memory distributed database designed for fast access to large volumes of data." \
      io.k8s.display-name="Infinispan Server" \
      io.openshift.expose-services="11222:hotrod" \
      io.openshift.tags="datagrid,java,jboss" \

ENV HOME /opt/jboss/

USER 1000

# Copy entrypoint script
COPY docker-entrypoint.sh /usr/local/bin

ENTRYPOINT ["docker-entrypoint.sh"]

# Expose Infinispan server  ports 
EXPOSE 7600 8080 8181 8888 9990 11211 11222 57600