package org.infinispan.query.remote;

/**
 * MBean interface for ProtobufMetadataManager,suitable for building invocation proxies with one of the {@link
 * javax.management.JMX#newMBeanProxy} methods.
 *
 * @author anistor@redhat.com
 * @author gustavonalle
 * @since 6.1
 * @deprecated Replaced by {@link org.infinispan.query.remote.client.ProtobufMetadataManagerMBean}
 */
@Deprecated
public interface ProtobufMetadataManagerMBean extends org.infinispan.query.remote.client.ProtobufMetadataManagerMBean {

   /**
    * @deprecated Replaced by {@link org.infinispan.query.remote.client.ProtobufMetadataManagerMBean#getProtofile}
    */
   @Deprecated
   String displayProtofile(String fileName);
}
