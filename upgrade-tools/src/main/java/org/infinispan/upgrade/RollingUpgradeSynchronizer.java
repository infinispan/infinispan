package org.infinispan.upgrade;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.io.ByteBuffer;
import org.infinispan.manager.CacheContainer;
import org.infinispan.marshall.BufferSizePredictor;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.util.ByteArrayKey;
import org.infinispan.util.FileLookup;
import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * // TODO: Document this
 *
 * @author Manik Surtani
 * @since 5.1
 */

// TODO: make this accessible via JMX on the new cluster as well.  Not just via the command-line with a boat load of jars!
public class RollingUpgradeSynchronizer {

   private final Properties oldCluster;
   private final Properties newCluster;
   private final String cacheName;
   private int threads;

   public static void main(String[] args) throws UnsupportedEncodingException {
      RollingUpgradeSynchronizer r = new RollingUpgradeSynchronizer(args);
      r.start();
   }

   public RollingUpgradeSynchronizer(String[] args) {
      if (args.length < 2)
         helpAndExit();

      String oldClusterCfg = args[0];
      String newClusterCfg = args[1];

      oldCluster = readProperties(oldClusterCfg);
      newCluster = readProperties(newClusterCfg);

      if (args.length >= 3)
         cacheName = args[2];
      else
         cacheName = CacheContainer.DEFAULT_CACHE_NAME;

      threads = Runtime.getRuntime().availableProcessors(); // default to the number of CPUs
      if (args.length >= 4) {
         try {
            threads = Integer.parseInt(args[3]);
         } catch (Exception e) {
            System.out.printf("  WARN: parameter %s should represent the nunber of threads to use, and be an integer. Using the default number of threads instead.%n", args[3]);
         }
      }
   }

   private static void helpAndExit() {
      System.out.println("  Usage: RollingUpgradeSynchronizer <old cluster properties file> <new cluster properties file> <cache name> <num threads to use>");
      System.out.println();
      System.out.println("         The last two parameters are optional, defaulting to the default cache and number of processors, respectively.");
      System.out.println();
      System.exit(0);
   }

   private static Properties readProperties(String propsFile) {
      try {
         Properties p = new Properties();
         FileLookup lookup = FileLookupFactory.newInstance();
         p.load(lookup.lookupFile(propsFile, RollingUpgradeSynchronizer.class.getClassLoader()));
         return p;
      } catch (Exception e) {
         System.out.printf("  FATAL: Unable to load properties file %s!  Exiting!%n", propsFile);
         System.exit(-1);
         return null;
      }
   }

   private void start() {
      long start = System.currentTimeMillis();
      // TODO: Take in more parameters, e.g., port, etc., possibly via a config file.
      // TODO: Should also take in a cache name, or even a set of cache names to migrate.
      Marshaller m = new MigrationMarshaller();

      RemoteCacheManager rcmOld = new RemoteCacheManager(m, oldCluster);
      final RemoteCacheManager rcmNew = new RemoteCacheManager(m, newCluster);

      Set<ByteArrayKey> keys = (Set<ByteArrayKey>) rcmOld.getCache(cacheName).get("___MigrationManager_HotRod_KnownKeys___");

      System.out.printf(">> Retrieved %s keys stored in cache %s on the old cluster.%n", keys.size(), cacheName);

      ExecutorService es = Executors.newFixedThreadPool(threads);

      final AtomicInteger count = new AtomicInteger(0);
      for (final ByteArrayKey key: keys) {
         es.submit(new Runnable() {
            @Override
            public void run() {
               // the custom marshaller registered above will make sure this byte array is placed, verbatim, on the stream
               rcmNew.getCache(cacheName).get(key.getData());
               int i = count.get();
               if (i % 100 == 0) System.out.printf(">>    Moved %s keys%n", i);
            }
         });
         count.getAndIncrement();
      }

      es.shutdown();
      while (!es.isShutdown()) LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(100, TimeUnit.MILLISECONDS));
      System.out.printf(">> Transferred %s entries in cache %s from the old cluster to the new, in %s%n", keys.size(), cacheName, Util.prettyPrintTime(System.currentTimeMillis() - start));
   }

   private static class MigrationMarshaller implements Marshaller {

      private final Marshaller delegate = new GenericJBossMarshaller();

      @Override
      public byte[] objectToByteBuffer(Object o, int i) throws IOException, InterruptedException {
         if (o instanceof byte[])
            return (byte[]) o;
         else
            return delegate.objectToByteBuffer(o, i);
      }

      @Override
      public byte[] objectToByteBuffer(Object o) throws IOException, InterruptedException {
         if (o instanceof byte[])
            return (byte[]) o;
         else
            return delegate.objectToByteBuffer(o);
      }

      @Override
      public Object objectFromByteBuffer(byte[] bytes) throws IOException, ClassNotFoundException {
         return delegate.objectFromByteBuffer(bytes);
      }

      @Override
      public Object objectFromByteBuffer(byte[] bytes, int i, int i1) throws IOException, ClassNotFoundException {
         return delegate.objectFromByteBuffer(bytes, i, i1);
      }

      @Override
      public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
         if (o instanceof byte[]) {
            byte[] bytes = (byte[]) o;
            return new ByteBuffer(bytes, 0, bytes.length);
         } else {
            return delegate.objectToBuffer(o);
         }
      }

      @Override
      public boolean isMarshallable(Object o) throws Exception {
         return o instanceof byte[] || delegate.isMarshallable(o);
      }

      @Override
      public BufferSizePredictor getBufferSizePredictor(Object o) {
         return delegate.getBufferSizePredictor(o);
      }
   }
}

