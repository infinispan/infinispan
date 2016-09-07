package org.infinispan.client.hotrod.event;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.io.Serializable;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 8.2
 */
@Test(groups = "functional", testName = "client.hotrod.event.ClientEventsOOMTest")
public class ClientEventsOOMTest extends MultiHotRodServersTest {

   private static final int NUM_ENTRIES = Integer.getInteger("client.stress.num_entries", 1000);
   private static final long SLEEP_TIME = Long.getLong("client.stress.sleep_time", 10); // ms

   private static final int NUM_NODES = 2;

   private static final int NUM_OWNERS = 2;

   private static BufferPoolMXBean DIRECT_POOL = getDirectMemoryPool();

   private RemoteCache<Integer, byte[]> remoteCache;

   // There is only one Godzilla in heap, but we can use Netty's off-heap pools to multiply them and destroy the world
   private static final byte[] GODZILLA = makeGodzilla();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfgBuilder = getConfigurationBuilder();
      createHotRodServers(NUM_NODES, cfgBuilder);

      waitForClusterToForm();

      for (int i = 0; i < NUM_NODES; i++) {
         server(i).addCacheEventConverterFactory("godzilla-growing-converter-factory", new CustomConverterFactory());
      }

      remoteCache = client(0).getCache();
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(NUM_OWNERS);
      //playing with OOM - weird things might happen when JVM will struggle for life
      builder.clustering().remoteTimeout(5, TimeUnit.MINUTES);
      return hotRodCacheConfiguration(builder);
   }

   public void testOOM() throws Throwable {
      try {
         log.debugf("Max direct memory is: %s%n", humanReadableByteCount(maxDirectMemory0(), false));

         logDirectMemory(log);

         byte[] babyGodzilla = new byte[]{13};
         for (int i = 0; i < NUM_ENTRIES; i++) {
            remoteCache.put(i, babyGodzilla);
         }
         log.debugf("ADDED %d BABY GODZILLAS\n", NUM_ENTRIES);

         log.debugf("ADDING LISTENER!");
         CountDownLatch latch = new CountDownLatch(1);
         ClientEntryListener listener = new ClientEntryListener(latch);
         logDirectMemory(log);
         remoteCache.addClientListener(listener);  // the world ends here
         log.debugf("ADDED LISTENER");
         logDirectMemory(log);

         latch.await(1, TimeUnit.MINUTES);

         remoteCache.removeClientListener(listener);
         assertEquals(NUM_ENTRIES, listener.eventCount);
      } catch(Throwable t) {
         log.debug("Exception reported, direct memory usage is:", t);
         logDirectMemory(log);
         throw t;
      }
   }

   private static void logDirectMemory(Log log) {
      log.debugf("Direct memory: used=%s, capacity=%s%n",
            humanReadableByteCount(DIRECT_POOL.getMemoryUsed(), false),
            humanReadableByteCount(DIRECT_POOL.getTotalCapacity(), false));
   }

   private static BufferPoolMXBean getDirectMemoryPool() {
      List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
      BufferPoolMXBean directPool = null;
      for (BufferPoolMXBean pool : pools) {
         if (pool.getName().equals("direct")) directPool = pool;
      }
      return directPool;
   }

   private static long maxDirectMemory0() {
      try {
         // Try to get from sun.misc.VM.maxDirectMemory() which should be most accurate.
         Class<?> vmClass = Class.forName("sun.misc.VM", true, ClassLoader.getSystemClassLoader());
         Method m = vmClass.getDeclaredMethod("maxDirectMemory");
         return ((Number) m.invoke(null)).longValue();
      } catch (Throwable t) {
         // Ignore
         return -1;
      }
   }

   private static String humanReadableByteCount(long bytes, boolean si) {
      int unit = si ? 1000 : 1024;
      if (bytes < unit) return bytes + " B";
      int exp = (int) (Math.log(bytes) / Math.log(unit));
      String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
      return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
   }

   @ClientListener(converterFactoryName = "godzilla-growing-converter-factory", useRawData = true, includeCurrentState = true)
   private static class ClientEntryListener {
      private static final Log log = LogFactory.getLog(ClientEntryListener.class);
      private final CountDownLatch latch;
      int eventCount = 0;

      ClientEntryListener(CountDownLatch latch) {
         this.latch = latch;
      }

      @ClientCacheEntryCreated
      @SuppressWarnings("unused")
      public void handleClientCacheEntryCreatedEvent(ClientCacheEntryCustomEvent event) {
         int length = ((byte[]) event.getEventData()).length;
         eventCount++;
         log.debugf("ClientEntryListener.handleClientCacheEntryCreatedEvent eventCount=%d length=%d\n", eventCount, length);
         logDirectMemory(log);
         if (eventCount == NUM_ENTRIES) latch.countDown();
         try {
            Thread.sleep(SLEEP_TIME);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
   }

   @NamedFactory(name = "godzilla-growing-converter-factory")
   private static class CustomConverterFactory implements CacheEventConverterFactory {
      @Override
      public <K, V, C> CacheEventConverter<K, V, C> getConverter(Object[] params) {
         return new CustomConverter<K, V, C>();
      }

      static class CustomConverter<K, V, C> implements CacheEventConverter<K, V, C>, Serializable {
         @Override
         public C convert(Object key, Object previousValue, Metadata previousMetadata, Object value, Metadata metadata, EventType eventType) {
            // all baby godzillas get converted to full grown godzillas
            return (C) GODZILLA;
         }
      }
   }

   private static byte[] makeGodzilla() {
      // this will not fit through the keyhole
      byte[] godzilla = new byte[1024 * 1024 * 42];
      Arrays.fill(godzilla, (byte) 13);
      return godzilla;
   }
}
