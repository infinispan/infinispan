package org.infinispan.query.remote;

/**
 * MBean interface for ProtobufMetadataManager.
 *
 * @author anistor@redhat.com
 * @since 6.1
 */
public interface ProtobufMetadataManagerMBean {

   void registerProtofile(byte[] descriptorFile);
}
