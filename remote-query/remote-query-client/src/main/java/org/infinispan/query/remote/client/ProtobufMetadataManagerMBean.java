package org.infinispan.query.remote.client;

/**
 * MBean interface for ProtobufMetadataManager,suitable for building invocation proxies with one of the {@link
 * javax.management.JMX#newMBeanProxy} methods.
 *
 * @author anistor@redhat.com
 * @author gustavonalle
 * @since 7.1
 */
public interface ProtobufMetadataManagerMBean extends ProtobufMetadataManagerConstants {

   void registerProtofile(String fileName, String contents) throws Exception;

   void registerProtofiles(String[] fileName, String[] contents) throws Exception;

   String[] getProtofileNames();

   String getProtofile(String fileName);

   String[] getFilesWithErrors();

   String getFileErrors(String fileName);
}
