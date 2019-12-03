package org.infinispan.it.endpoints;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.testng.Assert.assertEquals;

import java.io.IOException;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.infinispan.client.hotrod.RemoteCacheManager;
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
   protected RemoteCacheManager createRemoteCacheManager() throws IOException {
      SerializationContextInitializer sci = EndpointITSCI.INSTANCE;
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
            .addServer().host("localhost").port(hotRodServer.getPort())
            .addContextInitializer(sci)
            .build());

      //initialize server-side serialization context via rest endpoint
      String metadataCacheEndpoint = String.format("http://localhost:%s/rest/v2/caches/%s", restServer.getPort(), PROTOBUF_METADATA_CACHE_NAME);
      EntityEnclosingMethod put = new PutMethod(metadataCacheEndpoint + "/" + sci.getProtoFileName());
      put.setRequestEntity(new StringRequestEntity(sci.getProtoFile(), "text/plain", "UTF-8"));

      restClient.executeMethod(put);
      assertEquals(put.getStatusCode(), HttpStatus.SC_NO_CONTENT);

      return remoteCacheManager;
   }

}
