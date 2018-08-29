package org.infinispan.it.endpoints;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;

import java.io.IOException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.testng.annotations.Test;

/**
 * Tests for indexing json using object storage. The entity {@link CryptoCurrency} is annotated
 * with both Protobuf and Hibernate Search.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "it.endpoints.JsonPojoStoreTest")
public class JsonPojoStoreTest extends BaseJsonTest {

   @Override
   protected ConfigurationBuilder getIndexCacheConfiguration() {
      ConfigurationBuilder indexedCache = new ConfigurationBuilder();

      indexedCache.indexing().index(Index.PRIMARY_OWNER)
            .addProperty("default.directory_provider", "ram");

      indexedCache.encoding().key().mediaType(APPLICATION_OBJECT_TYPE);
      indexedCache.encoding().value().mediaType(APPLICATION_OBJECT_TYPE);

      return indexedCache;
   }

   @Override
   protected String getEntityName() {
      return CryptoCurrency.class.getName();
   }

   @Override
   protected RemoteCacheManager createRemoteCacheManager() throws IOException {
      return new RemoteCacheManager(new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
            .addServer().host("localhost").port(hotRodServer.getPort())
            .build());
   }

}
