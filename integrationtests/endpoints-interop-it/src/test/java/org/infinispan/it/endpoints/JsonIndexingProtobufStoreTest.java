package org.infinispan.it.endpoints;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import java.io.IOException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
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

      indexedCache.indexing().index(Index.PRIMARY_OWNER)
            .addProperty("default.directory_provider", "ram");

      indexedCache.encoding().key().mediaType("application/x-protostream");
      indexedCache.encoding().value().mediaType("application/x-protostream");

      return indexedCache;
   }

   @Override
   protected RemoteCacheManager createRemoteCacheManager() throws IOException {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
            .addServer().host("localhost").port(hotRodServer.getPort())
            .marshaller(new ProtoStreamMarshaller())
            .build());

      //initialize client-side serialization context
      SerializationContext serializationContext = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String protoFile = protoSchemaBuilder.fileName("crypto.proto")
            .addClass(CryptoCurrency.class)
            .build(serializationContext);

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("crypto.proto", protoFile);

      return remoteCacheManager;
   }

   protected String getEntityName() {
      return "CryptoCurrency";
   }


}
