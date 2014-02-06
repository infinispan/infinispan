package org.infinispan.client.hotrod.retry;

import org.infinispan.Cache;
import org.infinispan.affinity.KeyAffinityService;
import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.KeyGenerator;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransport;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.marshall.core.JBossMarshaller;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.retry.DistributionRetryTest", groups = "functional")
public class DistributionRetryTest extends AbstractRetryTest {

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numOwners(1);
      return builder;
   }

   public void testGet() throws Exception {
      log.info("Starting actual test");
      Object key = generateKeyAndShutdownServer();
      //now make sure that next call won't fail
      resetStats();
      assertEquals(remoteCache.get(key), "v");
   }

   public void testPut() throws Exception {
      Object key = generateKeyAndShutdownServer();
      log.info("Here it starts");
      assertEquals(remoteCache.put(key, "v0"), "v");
   }

   public void testRemove() throws Exception {
      Object key = generateKeyAndShutdownServer();
      assertEquals("v", remoteCache.remove(key));
   }

   public void testContains() throws Exception {
      Object key = generateKeyAndShutdownServer();
      resetStats();
      assertEquals(true, remoteCache.containsKey(key));
   }

   public void testGetWithVersion() throws Exception {
      Object key = generateKeyAndShutdownServer();
      resetStats();
      VersionedValue value = remoteCache.getVersioned(key);
      assertEquals("v", value.getValue());
   }

   public void testPutIfAbsent() throws Exception {
      Object key = generateKeyAndShutdownServer();
      assertEquals(null, remoteCache.putIfAbsent("noSuchKey", "someValue"));
      assertEquals("someValue", remoteCache.get("noSuchKey"));
   }

   public void testReplace() throws Exception {
      Object key = generateKeyAndShutdownServer();
      assertEquals("v", remoteCache.replace(key, "v2"));
   }

   public void testReplaceIfUnmodified() throws Exception {
      Object key = generateKeyAndShutdownServer();
      assertEquals(false, remoteCache.replaceWithVersion(key, "v2", 12));
   }

   public void testRemoveIfUnmodified() throws Exception {
      Object key = generateKeyAndShutdownServer();
      resetStats();
      assertEquals(false, remoteCache.removeWithVersion(key, 12));
   }

   public void testClear() throws Exception {
      Object key = generateKeyAndShutdownServer();
      resetStats();
      remoteCache.clear();
      assertEquals(false, remoteCache.containsKey(key));
   }

   private Object generateKeyAndShutdownServer() throws IOException, ClassNotFoundException, InterruptedException {
      resetStats();
      Cache<Object,Object> cache = manager(1).getCache();
      ExecutorService ex = Executors.newSingleThreadExecutor();
      KeyAffinityService kaf = KeyAffinityServiceFactory.newKeyAffinityService(cache, ex, new ByteKeyGenerator(), 2, true);
      Address address = cache.getAdvancedCache().getRpcManager().getTransport().getAddress();
      byte[] keyBytes = (byte[]) kaf.getKeyForAddress(address);
      String key = ByteKeyGenerator.getStringObject(keyBytes);
      ex.shutdownNow();
      kaf.stop();

      remoteCache.put(key, "v");
      assertOnlyServerHit(getAddress(hotRodServer2));
      TcpTransportFactory tcpTp = (TcpTransportFactory) TestingUtil.extractField(remoteCacheManager, "transportFactory");

      Marshaller sm = new JBossMarshaller();
      TcpTransport transport = (TcpTransport) tcpTp.getTransport(sm.objectToByteBuffer(key, 64), null);
      try {
      assertEquals(transport.getServerAddress(), new InetSocketAddress("localhost", hotRodServer2.getPort()));
      } finally {
         tcpTp.releaseTransport(transport);
      }


      log.info("About to stop Hot Rod server 2");
      hotRodServer2.stop();


      return key;
   }

   public static class ByteKeyGenerator implements KeyGenerator {
      Random r = new Random();
      @Override
      public Object getKey() {
         String result = String.valueOf(r.nextLong());
         Marshaller sm = new JBossMarshaller();
         try {
            return sm.objectToByteBuffer(result, 64);
         } catch (IOException e) {
            throw new RuntimeException(e);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      }

      public static String getStringObject(byte[] bytes) {
         try {
            Marshaller sm = new JBossMarshaller();
            return (String) sm.objectFromByteBuffer(bytes);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }

}
