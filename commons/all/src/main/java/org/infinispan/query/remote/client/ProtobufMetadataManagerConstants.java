package org.infinispan.query.remote.client;

import org.infinispan.commons.internal.InternalCacheNames;

/**
 * Useful constants used by the Protobuf metadata cache.
 *
 * @author anistor@redhat.com
 * @since 7.1
 * @deprecated since 16.0, use Schemas administration API instead.
 * <br/>
 * <code>
 *    RemoteCacheManager remoteCacheManager = ...;
 *    RemoteSchemasAdmin schemasAdmin = remoteCacheManager.administration().schemas();
 * </code>
 */
@Deprecated
public interface ProtobufMetadataManagerConstants {

   /**
    * The name of the Protobuf definitions cache.
    * @deprecated Use {@link InternalCacheNames#PROTOBUF_METADATA_CACHE_NAME}.
    */
   @Deprecated(forRemoval = true, since = "16.0")
   String PROTOBUF_METADATA_CACHE_NAME = InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME;

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
