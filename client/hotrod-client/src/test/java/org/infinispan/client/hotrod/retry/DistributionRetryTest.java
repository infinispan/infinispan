package org.infinispan.client.hotrod.retry;

import org.infinispan.Cache;
import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.KeyGenerator;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.impl.SerializationMarshaller;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "hotrod.retry.DistributionRetryTest", groups = "functional")
public class DistributionRetryTest extends AbstractRetryTest {

   @Override
   protected Configuration getCacheConfig() {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      config.setNumOwners(1);
      return config;
   }

   @Override
   protected void waitForClusterToForm() {
      super.waitForClusterToForm();
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(0), cache(1), cache(2));
   }

   public void testGet() {
      log.info("Starting actual test");
      Object key = generateKeyAndShutdownServer();
      //now make sure that next call won't fail
      resetStats();
      assertEquals(remoteCache.get(key), "v");
   }

   public void testPut() {
      Object key = generateKeyAndShutdownServer();
      log.info("Here it starts");
      assertEquals(remoteCache.put(key, "v0"), "v");
   }

   public void testRemove() {
      Object key = generateKeyAndShutdownServer();
      assertEquals("v", remoteCache.remove(key));
   }

   public void testContains() {
      Object key = generateKeyAndShutdownServer();
      resetStats();
      assertEquals(true, remoteCache.containsKey(key));
   }

   public void testGetWithVersion() {
      Object key = generateKeyAndShutdownServer();
      resetStats();
      VersionedValue value = remoteCache.getVersioned(key);
      assertEquals("v", value.getValue());
   }

   public void testPutIfAbsent() {
      Object key = generateKeyAndShutdownServer();
      assertEquals(null, remoteCache.putIfAbsent("noSuchKey", "someValue"));
      assertEquals("someValue", remoteCache.get("noSuchKey"));
   }

   public void testReplace() {
      Object key = generateKeyAndShutdownServer();
      assertEquals("v", remoteCache.replace(key, "v2"));
   }

   public void testReplaceIfUnmodified() {
      Object key = generateKeyAndShutdownServer();
      assertEquals(false, remoteCache.replaceWithVersion(key, "v2", 12));
   }

   public void testRemoveIfUnmodified() {
      Object key = generateKeyAndShutdownServer();
      resetStats();
      assertEquals(false, remoteCache.removeWithVersion(key, 12));
   }

   public void testClear() {
      Object key = generateKeyAndShutdownServer();
      resetStats();
      remoteCache.clear();
      assertEquals(false, remoteCache.containsKey(key));
   }

   private Object generateKeyAndShutdownServer() {
      resetStats();
      Cache<Object,Object> cache = manager(1).getCache();
      KeyAffinityService kaf = KeyAffinityServiceFactory.newKeyAffinityService(cache, Executors.newSingleThreadExecutor(), new ByteKeyGenerator(), 2, true);
      Address address = cache.getAdvancedCache().getRpcManager().getTransport().getAddress();
      byte[] keyBytes = (byte[]) kaf.getKeyForAddress(address);
      String key = ByteKeyGenerator.getStringObject(keyBytes);
      kaf.stop();

      remoteCache.put(key, "v");
      assertOnlyServerHit(getAddress(hotRodServer2));
      TcpTransportFactory tcpTp = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");

      SerializationMarshaller sm = new SerializationMarshaller();
      TcpTransport transport = (TcpTransport) tcpTp.getTransport(sm.marshallObject(key));
      try {
      assertEquals(transport.getServerAddress(), new InetSocketAddress("localhost", hotRodServer2.getPort()));
      } finally {
         tcpTp.releaseTransport(transport);
      }
      

      log.info("About to stop hotrod server 2");
      hotRodServer2.stop();


      return key;
   }

   static class ByteKeyGenerator implements KeyGenerator {
      Random r = new Random();
      @Override
      public Object getKey() {
         String result = String.valueOf(r.nextLong());
         SerializationMarshaller sm = new SerializationMarshaller();
         return sm.marshallObject(result);
      }

      static String getStringObject(byte[] bytes) {
         SerializationMarshaller sm = new SerializationMarshaller();
         return (String) sm.readObject(bytes);
      }
   }

}
