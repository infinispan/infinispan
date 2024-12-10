package org.infinispan.it.endpoints;

import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.testng.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.SerializationContextInitializer;
import org.testng.annotations.Test;

/**
 * Tests interoperability between rest and hot rod for json indexing and querying, with the
 * schema registration done via rest
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "it.endpoints.ProtoRegistrationJsonTest")
public class ProtoRegistrationJsonTest extends JsonIndexingProtobufStoreTest {

   @Override
   protected RemoteCacheManager createRemoteCacheManager() {
      SerializationContextInitializer sci = EndpointITSCI.INSTANCE;
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
            .addServer().host("localhost").port(hotRodServer.getPort())
            .addContextInitializer(sci)
            .build());

      //initialize server-side serialization context via rest endpoint
      RestEntity protoFile = RestEntity.create(MediaType.TEXT_PLAIN, ((GeneratedSchema) sci).getProtoFile());
      RestResponse response = join(restClient.cache(PROTOBUF_METADATA_CACHE_NAME).put(((GeneratedSchema) sci).getProtoFileName(), protoFile));
      assertEquals(response.status(), 204);

      return remoteCacheManager;
   }

}
