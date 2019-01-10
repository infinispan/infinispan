package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 9.0
 */
@Test(testName = "client.hotrod.StreamingOpsTest", groups = "functional")
public class StreamingOpsTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(StreamingOpsTest.class);

   private static final String CACHE_NAME = "theCache";
   private static final int V1_SIZE = 2_000;
   private static final int V2_SIZE = 1_000;

   RemoteCache<String, byte[]> remoteCache;
   StreamingRemoteCache<String> streamingRemoteCache;
   private RemoteCacheManager remoteCacheManager;

   protected HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultStandaloneCacheConfig(false));
      EmbeddedCacheManager cm = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration());
      cm.defineConfiguration(CACHE_NAME, builder.build());
      cm.getCache(CACHE_NAME);
      return cm;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      //pass the config file to the cache
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());

      remoteCacheManager = getRemoteCacheManager();
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);
      streamingRemoteCache = remoteCache.streaming();
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
      return new RemoteCacheManager(clientBuilder.build());
   }

   @AfterClass
   public void testDestroyRemoteCacheFactory() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
   }

   private void consumeAndCloseStream(InputStream is) throws Exception {
      if (is != null) {
         try {
            while (is.read() >= 0) {
               //consume
            }
         } finally {
            is.close();
         }
      }
   }

   private void writeDataToStream(OutputStream os, int length) throws Exception {
      for (int i = 0; i < length; i++) {
         os.write(i % 256);
      }
   }

   public void testPutGetStream() throws Exception {
      OutputStream k1os = streamingRemoteCache.put("k1");
      writeDataToStream(k1os, V1_SIZE);
      k1os.close();

      InputStream k1is = streamingRemoteCache.get("k1");
      int count = readAndCheckDataFromStream(k1is);
      assertEquals(V1_SIZE, count);
   }

   private int readAndCheckDataFromStream(InputStream k1is) throws IOException {
      int count = 0;
      try {
         for (int b = k1is.read(); b >= 0; b = k1is.read(), count++) {
            assertEquals(count % 256, b);
         }
      } finally {
         k1is.close();
      }
      return count;
   }


   public void testGetStreamWithMetadata() throws Exception {
      InputStream k1is = streamingRemoteCache.get("k1");
      assertNull("expected null but received a stream", k1is);
      consumeAndCloseStream(k1is);

      OutputStream k1os = streamingRemoteCache.put("k1");
      writeDataToStream(k1os, V1_SIZE);
      k1os.close();

      k1is = streamingRemoteCache.get("k1");
      assertNotNull("expected a stream but received null", k1is);
      VersionedMetadata k1metadata = (VersionedMetadata) k1is;
      assertEquals(-1, k1metadata.getLifespan());
      assertEquals(-1, k1metadata.getMaxIdle());
      consumeAndCloseStream(k1is);

      k1os = streamingRemoteCache.put("k1", 5, TimeUnit.MINUTES);
      writeDataToStream(k1os, V1_SIZE);
      k1os.close();
      k1is = streamingRemoteCache.get("k1");
      assertNotNull("expected a stream but received null", k1is);
      k1metadata = (VersionedMetadata) k1is;
      assertEquals(TimeUnit.MINUTES.toSeconds(5), k1metadata.getLifespan());
      assertEquals(-1, k1metadata.getMaxIdle());
      consumeAndCloseStream(k1is);

      k1os = streamingRemoteCache.put("k1", 5, TimeUnit.MINUTES, 3, TimeUnit.MINUTES);
      writeDataToStream(k1os, V1_SIZE);
      k1os.close();
      k1is = streamingRemoteCache.get("k1");
      assertNotNull("expected a stream but received null", k1is);
      k1metadata = (VersionedMetadata) k1is;
      assertEquals(TimeUnit.MINUTES.toSeconds(5), k1metadata.getLifespan());
      assertEquals(TimeUnit.MINUTES.toSeconds(3), k1metadata.getMaxIdle());
      consumeAndCloseStream(k1is);
   }

   public void testPutIfAbsentStream() throws Exception {
      InputStream k1is = streamingRemoteCache.get("k1");
      assertNull("expected null but received a stream", k1is);
      consumeAndCloseStream(k1is);

      // Write a V1 value
      OutputStream k1os = streamingRemoteCache.putIfAbsent("k1");
      writeDataToStream(k1os, V1_SIZE);
      k1os.close();

      k1is = streamingRemoteCache.get("k1");
      assertEquals(V1_SIZE, readAndCheckDataFromStream(k1is));

      // Attempt to overwrite it with a V2 value
      k1os = streamingRemoteCache.putIfAbsent("k1");
      writeDataToStream(k1os, V2_SIZE);
      k1os.close();

      // Check that the value was not replaced
      k1is = streamingRemoteCache.get("k1");
      assertEquals(V1_SIZE, readAndCheckDataFromStream(k1is));
   }

   public void testReplaceStream() throws Exception {
      // Write a V1 value
      OutputStream k1os = streamingRemoteCache.putIfAbsent("k1");
      writeDataToStream(k1os, V1_SIZE);
      k1os.close();

      InputStream k1is = streamingRemoteCache.get("k1");
      assertEquals(V1_SIZE, readAndCheckDataFromStream(k1is));
      long version = ((VersionedMetadata)k1is).getVersion();
      assertTrue("Expected a non-zero version: " + version, version > 0);

      // Attempt to overwrite it by using a wrong version
      k1os = streamingRemoteCache.replaceWithVersion("k1", version + 1);
      writeDataToStream(k1os, V2_SIZE);
      k1os.close();

      // Check that the value was not replaced
      k1is = streamingRemoteCache.get("k1");
      assertEquals(V1_SIZE, readAndCheckDataFromStream(k1is));

      // Attempt to overwrite it by using the correct version
      k1os = streamingRemoteCache.replaceWithVersion("k1", version);
      writeDataToStream(k1os, V2_SIZE);
      k1os.close();

      // Check that the value was replaced
      k1is = streamingRemoteCache.get("k1");
      assertEquals(V2_SIZE, readAndCheckDataFromStream(k1is));
   }

}
