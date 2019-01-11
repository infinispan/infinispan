package org.infinispan.query.remote.client;

/**
 * Useful constants used by the Protobuf metadata cache.
 *
 * @author anistor@redhat.com
 * @since 7.1
 */
public interface ProtobufMetadataManagerConstants {

   /**
    * The name of the Protobuf definitions cache.
    */
   String PROTOBUF_METADATA_CACHE_NAME = "___protobuf_metadata";

   /**
    * All error status keys end with this suffix. This is also the name of the global error key.
    */
   String ERRORS_KEY_SUFFIX = ".errors";

   /**
    * All protobuf definition source files must end with this suffix.
    */
   String PROTO_KEY_SUFFIX = ".proto";

   /**
    * The 'component' key property of ProtobufMetadataManager's ObjectName.
    */
   String OBJECT_NAME = "ProtobufMetadataManager";
}
