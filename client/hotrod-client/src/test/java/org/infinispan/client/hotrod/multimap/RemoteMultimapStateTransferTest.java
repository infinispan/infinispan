package org.infinispan.client.hotrod.multimap;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

/**
 * State transfer test for multimap to ensure the Bucket class is properly sent.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "functional", testName = "client.hotrod.multimap.RemoteMultimapStateTransferTest")
public class RemoteMultimapStateTransferTest extends MultiHotRodServersTest {

   private static final int NODES = 2;
   private static final int VALUES = 4;
   private static final String CACHE_NAME = "multimap";

   private ClientIntelligence clientIntelligence;

   public RemoteMultimapStateTransferTest clientIntelligence(ClientIntelligence clientIntelligence) {
      this.clientIntelligence = clientIntelligence;
      return this;
   }

   @Override
   public Object[] factory() {
      return Arrays.stream(ClientIntelligence.values())
            .flatMap(ci -> Stream.of(CacheMode.DIST_SYNC, CacheMode.DIST_ASYNC)
                  .map(cm -> new RemoteMultimapStateTransferTest().clientIntelligence(ci).cacheMode(cm)))
            .toArray();
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), clientIntelligence);
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "clientIntelligence");
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_ASYNC, false);
      cacheBuilder.statistics().enable();
      createHotRodServers(NODES, new ConfigurationBuilder());
      defineInAll(CACHE_NAME, cacheBuilder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder cb =  super.createHotRodClientConfigurationBuilder(host, serverPort);
      cb.clientIntelligence(clientIntelligence);
      return cb;
   }

   public void testStateTransfer() {
      RemoteMultimapCache<String, String> mc = multimapCache(0);

      List<String> values1 = createValues();
      String key1 = Util.threadLocalRandomUUID().toString();

      storeValues(mc, key1, values1);

      List<String> values2 = createValues();
      String key2 = Util.threadLocalRandomUUID().toString();

      storeValues(mc, key2, values2);

      for (int i = 0; i < NODES; ++i) {
         assertData(i, key1, values1);
         assertData(i, key2, values2);
      }

      HotRodServer server = addHotRodServerAndClient(new ConfigurationBuilder());
      defineCache(server, CACHE_NAME, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));

      for (int i = 0; i < NODES + 1; ++i) {
         assertData(i, key1, values1);
         assertData(i, key2, values2);
      }
   }

   private RemoteMultimapCache<String, String> multimapCache(int index) {
      MultimapCacheManager<String, String> mcm = RemoteMultimapCacheManagerFactory.from(client(index));
      return mcm.get(CACHE_NAME);
   }

   private void assertData(int index, String key, List<String> values) {
      RemoteMultimapCache<String, String> mc = multimapCache(index);
      Collection<String> data = mc.get(key).join();
      assertEquals(values.size(), data.size());
      for (String v : values) {
         assertTrue(data.contains(v));
      }
   }

   private static void storeValues(RemoteMultimapCache<String, String> rmc, String key, List<String> values) {
      for (String v : values) {
         rmc.put(key, v).join();
      }
   }

   private static List<String> createValues() {
      List<String> values = new ArrayList<>(VALUES);
      for (int i = 0; i < VALUES; ++i) {
         values.add(Util.threadLocalRandomUUID().toString());
      }
      return values;
   }

}
