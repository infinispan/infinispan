package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Verifying the functionality of Queries using ISPN directory_provider on clustered hotrod server configuration.
 *
 * @author Anna Manukyan
 */
@Test(testName = "client.hotrod.query.MultiHotRodServerIspnDirQueryTest", groups = "functional", enabled = false, description = "Enable tests when ISPN-3672 is fixed.")
public class MultiHotRodServerIspnDirQueryTest extends MultiHotRodServerQueryTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      builder.indexing().enable().indexLocalOnly(false)
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      createHotRodServers(2, defaultConfiguration);

      //initialize server-side serialization context
      for (EmbeddedCacheManager cm : cacheManagers) {
         cm.defineConfiguration("queryableCache", builder.build());

         cm.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class).registerProtofile("/bank.protobin");
      }

      //initialize client-side serialization context
      for (RemoteCacheManager rcm : clients) {
         MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(rcm));
      }

      remoteCache0 = client(0).getCache("queryableCache");
      remoteCache1 = client(1).getCache("queryableCache");
   }

   @Test(enabled = false)
   public void testAttributeQuery() throws Exception {

   }

   @Test(enabled = false)
   public void testEmbeddedAttributeQuery() throws Exception {
   }

   @Test(enabled = false)
   public void testProjections() throws Exception {
   }
}
