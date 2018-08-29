package org.infinispan.it.endpoints;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.testng.Assert.assertEquals;

import java.io.IOException;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;
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

      //initialize server-side serialization context via rest endpoint
      String metadataCacheEndpoint = String.format("http://localhost:%s/rest/%s", restServer.getPort(), PROTOBUF_METADATA_CACHE_NAME);
      EntityEnclosingMethod put = new PutMethod(metadataCacheEndpoint + "/crypto.proto");
      put.setRequestEntity(new StringRequestEntity(protoFile, "text/plain", "UTF-8"));

      restClient.executeMethod(put);
      assertEquals(put.getStatusCode(), HttpStatus.SC_OK);

      return remoteCacheManager;
   }

}
