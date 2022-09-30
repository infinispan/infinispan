package org.infinispan.client.hotrod.size;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.model.Essay;
import org.infinispan.client.hotrod.annotation.model.EssayMarshaller;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.size.HugeProtobufMessageTest")
public class HugeProtobufMessageTest extends SingleHotRodServerTest {

   public static final int SIZE = 68_000_000; // use something that is > 64M (67,108,864)

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().key().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      builder.encoding().value().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);

      manager.defineConfiguration("homeworks", builder.build());
      return manager;
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      try {
         initProtoSchema(remoteCacheManager);
      } catch (IOException e) {
         fail("unexpected exception: " + e.getMessage());
      }

      return remoteCacheManager;
   }

   protected void initProtoSchema(RemoteCacheManager remoteCacheManager) throws IOException {
      //initialize client-side serialization context
      SerializationContext serCtx = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      MarshallerRegistration.registerMarshallers(serCtx);
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("essay.proto", EssayMarshaller.PROTO_SCHEMA));
      serCtx.registerMarshaller(new EssayMarshaller());
   }

   @Test
   public void testSearches() {
      RemoteCache<Integer, Essay> remoteCache = remoteCacheManager.getCache("homeworks");

      remoteCache.put(1, new Essay("my-very-extensive-essay", makeHugeString()));

      Essay essay = remoteCache.get(1);
      assertTrue(essay != null);
   }

   private String makeHugeString() {
      char[] chars = new char[SIZE];
      for (int i = 0; i < SIZE; i++) {
         char delta = (char) (i % 20);
         chars[i] = (char) ('a' + delta);
      }

      return new String(chars);
   }
}
