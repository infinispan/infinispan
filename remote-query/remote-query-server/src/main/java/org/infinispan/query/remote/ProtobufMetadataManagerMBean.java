package org.infinispan.query.remote;

/**
 * MBean interface for ProtobufMetadataManager.
 *
 * @author anistor@redhat.com
 * @author gustavonalle
 * @since 6.1
 */
public interface ProtobufMetadataManagerMBean {

   void registerProtofile(String name, String contents) throws Exception;

   void registerProtofiles(String[] name, String[] contents) throws Exception;

   String displayProtofile(String name);
}
