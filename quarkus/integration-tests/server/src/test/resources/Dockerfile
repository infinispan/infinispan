FROM registry.access.redhat.com/ubi9/ubi-minimal
ENV ISPN_HOME /opt/infinispan
WORKDIR /opt/infinispan
COPY opt /opt
RUN chmod -R 775 /opt
USER 185
EXPOSE 2157 7800 11221 11222 45700 57600