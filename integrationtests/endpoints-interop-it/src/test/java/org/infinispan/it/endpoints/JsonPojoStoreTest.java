package org.infinispan.it.endpoints;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests for indexing json using object storage. The entity {@link CryptoCurrency} is annotated with both Protobuf and
 * Hibernate Search.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "it.endpoints.JsonPojoStoreTest")
public class JsonPojoStoreTest extends BaseJsonTest {

   @Override
   protected ConfigurationBuilder getIndexCacheConfiguration() {
      ConfigurationBuilder indexedCache = new ConfigurationBuilder();

      indexedCache.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntities(CryptoCurrency.class);

      indexedCache.encoding().key().mediaType(APPLICATION_OBJECT_TYPE);
      indexedCache.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);

      return indexedCache;
   }

   @Override
   protected RemoteCacheManager createRemoteCacheManager() {
      return new RemoteCacheManager(new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
            .addServer().host("localhost").port(hotRodServer.getPort())
            .addContextInitializers(EndpointITSCI.INSTANCE)
            .build());
   }

   @Override
   protected String getJsonType() {
      return CryptoCurrency.class.getName();
   }
}
