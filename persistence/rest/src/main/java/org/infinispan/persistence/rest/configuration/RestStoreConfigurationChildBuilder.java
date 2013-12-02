package org.infinispan.persistence.rest.configuration;

import org.infinispan.configuration.cache.StoreConfigurationChildBuilder;
import org.infinispan.persistence.rest.metadata.EmbeddedMetadataHelper;
import org.infinispan.persistence.rest.metadata.MetadataHelper;
import org.infinispan.persistence.keymappers.MarshallingTwoWayKey2StringMapper;

/**
 * RestStoreConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public interface RestStoreConfigurationChildBuilder<S> extends StoreConfigurationChildBuilder<S> {

   /**
    * Configures the connection pool
    */
   ConnectionPoolConfigurationBuilder connectionPool();

   /**
    * The host to connect to
    */
   RestStoreConfigurationBuilder host(String  host);

   /**
    * The class name of a {@link org.infinispan.persistence.keymappers.Key2StringMapper} to use for mapping keys to strings suitable for
    * RESTful retrieval/storage. Defaults to {@link org.infinispan.persistence.keymappers.MarshalledValueOrPrimitiveMapper}
    */
   RestStoreConfigurationBuilder key2StringMapper(String key2StringMapper);

   /**
    * The class of a {@link org.infinispan.persistence.keymappers.Key2StringMapper} to use for mapping keys to strings suitable for
    * RESTful retrieval/storage. Defaults to {@link org.infinispan.persistence.keymappers.MarshalledValueOrPrimitiveMapper}
    */
   RestStoreConfigurationBuilder key2StringMapper(Class<? extends MarshallingTwoWayKey2StringMapper> klass);

   /**
    * The class name of a {@link MetadataHelper} to use for managing appropriate metadata for the entries
    * Defaults to {@link EmbeddedMetadataHelper}
    */
   RestStoreConfigurationBuilder metadataHelper(String metadataHelper);

   /**
    * The class of a {@link MetadataHelper} to use for managing appropriate metadata for the entries
    * Defaults to {@link EmbeddedMetadataHelper}
    */
   RestStoreConfigurationBuilder metadataHelper(Class<? extends MetadataHelper> metadataHelper);

   /**
    * The path portion of the RESTful service. Defaults to /
    */
   RestStoreConfigurationBuilder path(String path);

   /**
    * The port to connect to. Defaults to 80
    */
   RestStoreConfigurationBuilder port(int port);

   /**
    * Determines whether to append the cache name to the path URI. Defaults to false.
    */
   RestStoreConfigurationBuilder appendCacheNameToPath(boolean appendCacheNameToPath);
}
