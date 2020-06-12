package org.infinispan.it.endpoints;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import java.io.IOException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.testng.annotations.Test;

/**
 * Test for indexing json using protobuf underlying storage without forcing unmarshalled storage.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "it.endpoints.JsonIndexingProtobufStoreTest")
public class JsonIndexingProtobufStoreTest extends BaseJsonTest {

   @Override
   protected ConfigurationBuilder getIndexCacheConfiguration() {
      ConfigurationBuilder indexedCache = new ConfigurationBuilder();

      indexedCache.indexing().enable()
                  .addIndexedEntity("org.infinispan.test.endpoint.it.CryptoCurrency")
                  .addProperty("default.directory_provider", "local-heap");

      indexedCache.encoding().key().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      indexedCache.encoding().value().mediaType(APPLICATION_PROTOSTREAM_TYPE);

      return indexedCache;
   }

   @Override
   protected RemoteCacheManager createRemoteCacheManager() throws IOException {
      SerializationContextInitializer sci = EndpointITSCI.INSTANCE;
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
            .addServer().host("localhost").port(hotRodServer.getPort())
            .addContextInitializer(sci)
            .build());

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put(sci.getProtoFileName(), sci.getProtoFile());

      return remoteCacheManager;
   }
}
