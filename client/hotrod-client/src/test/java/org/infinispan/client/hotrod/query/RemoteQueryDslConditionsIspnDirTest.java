package org.infinispan.client.hotrod.query;

import org.hibernate.search.infinispan.InfinispanIntegration;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verifying the functionality of remote queries for infinispan directory_provider.
 *
 * @author Anna Manukyan
 */
@Test(testName = "client.hotrod.query.RemoteQueryDslConditionsIspnDirTest", groups = "functional")
@CleanupAfterMethod
public class RemoteQueryDslConditionsIspnDirTest extends RemoteQueryDslConditionsTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getConfigurationBuilder();

      cacheManager = TestCacheManagerFactory.createCacheManager(builder);

      ConfigurationBuilder defaultCacheConfiguration = new ConfigurationBuilder();
      cacheManager.defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, defaultCacheConfiguration.build());
      cacheManager.defineConfiguration(InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, defaultCacheConfiguration.build());
      cacheManager.defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME, defaultCacheConfiguration.build());

      cache = cacheManager.getCache();

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();

      //initialize server-side serialization context
      cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class).registerProtofile("/bank.protobin");

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));

      return cacheManager;
   }

   public String getDirectoryProvider() {
      return "infinispan";
   }

}
