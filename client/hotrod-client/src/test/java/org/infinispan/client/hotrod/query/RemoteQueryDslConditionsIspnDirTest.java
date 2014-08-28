package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.testng.annotations.Test;

/**
 * Verifying the functionality of remote queries for infinispan directory_provider.
 *
 * @author Anna Manukyan
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.RemoteQueryDslConditionsIspnDirTest", groups = "functional")
public class RemoteQueryDslConditionsIspnDirTest extends RemoteQueryDslConditionsTest {

   protected static final String TEST_CACHE_NAME = "testCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultCacheConfiguration = new ConfigurationBuilder();
      createClusteredCaches(1, defaultCacheConfiguration);

      ConfigurationBuilder cfg = getConfigurationBuilder();
      manager(0).defineConfiguration(TEST_CACHE_NAME, cfg.build());
      cache = manager(0).getCache(TEST_CACHE_NAME);

      hotRodServer = TestHelper.startHotRodServer(manager(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);

      //initialize server-side serialization context
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.read(Util.getResourceAsStream("/sample_bank_account/bank.proto", getClass().getClassLoader())));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = super.getConfigurationBuilder();
      builder.indexing().addProperty("default.directory_provider", "infinispan");
      return builder;
   }
}
