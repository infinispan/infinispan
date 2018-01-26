package org.infinispan.server.test.client.hotrod;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.StreamingRemoteCache;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * The basic set of tests for Streaming API over HottRod client
 *
 * @author zhostasa
 *
 */
@RunWith(Arquillian.class)
@WithRunningServer({ @RunningServer(name = "default-clustered-manual-1"),
      @RunningServer(name = "default-clustered-manual-2") })
public class HotRodRemoteStreamingIT {
   private static final String SERVER_1_NAME = "default-clustered-manual-1",
         SERVER_2_NAME = "default-clustered-manual-2";

   private static final String USED_MEMORY_KEY = "used";

   @InfinispanResource(SERVER_1_NAME)
   private static RemoteInfinispanServer server1;
   private static RemoteCacheManager rcm1;

   @InfinispanResource(SERVER_2_NAME)
   private static RemoteInfinispanServer server2;
   private static RemoteCacheManager rcm2;

   private StreamingRemoteCache<Object> src1, src2;

   @ArquillianResource
   private ContainerController controller;

   private Configuration conf1, conf2;

   private Boolean finalized = new Boolean(false);

   private static Random random = new Random();

   /**
    * Refresh the resources for each test
    */
   @Before
   public void setUp() {

      if (conf1 == null || conf2 == null) {
         conf1 = new ConfigurationBuilder().addServer().host(server1.getHotrodEndpoint().getInetAddress().getHostName())
               .port(server1.getHotrodEndpoint().getPort()).build();

         conf2 = new ConfigurationBuilder().addServer().host(server2.getHotrodEndpoint().getInetAddress().getHostName())
               .port(server2.getHotrodEndpoint().getPort()).build();
      }

      rcm1 = new RemoteCacheManager(conf1);
      rcm2 = new RemoteCacheManager(conf2);

      src1 = rcm1.getCache("streamingTestCache").streaming();
      src2 = rcm2.getCache("streamingTestCache").streaming();
   }

   /**
    * Check the server status for tests which involve shutting servers down
    */
   private void checkServers() {
      if (!controller.isStarted(SERVER_1_NAME)) {
         controller.start(SERVER_1_NAME);
      }
      if (!controller.isStarted(SERVER_2_NAME)) {
         controller.start(SERVER_2_NAME);
      }
   }

   /**
    * Test for basic functionality on multiple streams
    *
    * @throws Exception
    */
   @Test
   public void testBasicFunctionality() throws Exception {
      // this value is good balance between good test, length and memory
      // consumption, increase may necessitate JVm memory adjustment
      int streamCount = 100;

      List<RandomInserter> inserterList = new ArrayList<RandomInserter>();

      for (int i = 0; i < streamCount; i++) {
         inserterList.add(new RandomInserter(random.nextLong(), i % 2 == 0 ? src1 : src2, i % 2 == 0 ? src2 : src1));
      }

      boolean result = true;

      while (result) {
         Collections.shuffle(inserterList);

         for (RandomInserter r : inserterList) {
            result = result && r.process();
         }
         result = !result;
      }
   }

   /**
    * Test behaviour of cache if stream object if garbageCollected
    *
    * @throws Exception
    */
   @Test
   public void testGCOpenStream() throws Exception {

      Long seed = random.nextLong();
      RandomInserter ri = new RandomInserter(seed, src1, src1);

      ri.process();
      ri.process();

      ri = null;

      System.gc();

      for (int i = 0; i < 10; i++) {
         if (isFinalized())
            break;
         Thread.sleep(1000);
      }
      if (!isFinalized())
         fail("Testing object was not garbage collected in time limit");

      assertNull("Partial object found in cache1", src1.get(seed));
      assertNull("Partial object found in cache2", src2.get(seed));

   }

   /**
    * Test stream reaction on negative one value in stream
    *
    * @throws IOException
    */
   @Test
   public void testNegativeOneInStream() throws IOException {

      Long seed = random.nextLong();

      byte[] ba = new byte[1000];
      random.nextBytes(ba);
      for (int i = 1; i < 100; i++) {
         ba[i * 10] = -1;
      }

      OutputStream out = src1.put(seed);

      for (int i = 0; i < 1000; i++) {
         out.write(ba[i]);
      }
      out.close();

      InputStream in = src2.get(seed);

      for (int i = 0; i < 1000; i++) {
         assertEquals(ba[i], (byte) in.read());
      }

      in.close();
   }

   /**
    * Test cache behaviour if same key is being manipulated multiple times
    *
    * @throws IOException
    */
   @Test
   public void testKeyConcurency() throws IOException {
      int val1 = 123, val2 = 234;

      Long seed = random.nextLong();

      OutputStream out1 = src1.put(seed);

      out1.write(val1);

      OutputStream out2 = src2.put(seed);

      out2.write(val2);

      out1.close();
      out2.close();

      InputStream in = src1.get(seed);

      assertEquals("", val2, in.read());

      in.close();

      seed = random.nextLong();

      out1 = src1.putIfAbsent(seed);

      out1.write(val1);

      out2 = src2.putIfAbsent(seed);

      out2.write(val2);

      out1.close();
      out2.close();

      in = src1.get(seed);

      assertEquals("", val1, in.read());

      in.close();

   }

   /**
    * Test correct behaviour for RemoteCacheManagers start/stop function <br>
    * Current operations can be completed, but no new operations are supposed to
    * be issued
    *
    * @throws IOException
    * @throws InterruptedException
    */
   @Test
   public void RCMStopTest() throws IOException, InterruptedException {
      byte[] value = new byte[100];
      byte[] ret = new byte[100];
      random.nextBytes(value);

      Long key = random.nextLong();

      OutputStream out = src1.put(key);

      out.write(value, 0, 50);

      rcm1.stop();

      out.write(value, 50, 50);
      out.close();

      try {
         src1.get(key);
         src1.put(key);
         fail("No exception returned with stopped RemoteCacheManager");
      } catch (Exception e) {
      }

      rcm1.start();

      src1.get(key).read(ret);

      assertTrue("Returned value incorrect", Arrays.equals(ret, value));

   }

   /**
    * Test behaviour if one server is gracefully shutdown
    *
    * @throws IOException
    * @throws InterruptedException
    */
   @Test
   @Ignore("ISPN-8724")
   public void serverShutdownTest() throws IOException, InterruptedException {
      byte[] value = new byte[5000];
      random.nextBytes(value);
      try {
         for (int i = 0; i < 10; i++) {
            Long key = random.nextLong();

            OutputStream out = src1.put(key);

            out.write(value);
            out.write(value);

            stopServer(i);

            try {
               out.write(value);
               out.close();
            } catch (Exception e) {

               startServer(i);
               if (src1.get(key) == null) {
                  continue;
               } else
                  fail("Failed key found in te cache");

            }

            startServer(i);

         }
      } finally {
         checkServers();
      }
   }

   /**
    * Test behaviour if one server is killed
    *
    * @throws IOException
    * @throws InterruptedException
    */
   @Test
   @Ignore("ISPN-8724")
   public void serverKillTest() throws IOException, InterruptedException {
      byte[] value = new byte[5000];
      random.nextBytes(value);
      try {
         for (int i = 0; i < 2; i++) {
            Long key = random.nextLong();

            OutputStream out = src1.put(key);

            out.write(value);
            out.write(value);

            killServer(i);

            try {
               out.write(value);
               out.close();
            } catch (Exception e) {
               startServer(i);
               if (src1.get(key) == null) {
                  continue;
               } else
                  fail("Failed key found in te cache");

            }

            startServer(i);

         }
      } finally {
         checkServers();
      }
   }

   /**
    * Test basic memory consumption difference between standard API and
    * streaming API
    *
    * @throws Exception
    */
   @Test
   @Ignore
   public void performanceTest() throws Exception {
      try {
         byte[] ba = new byte[1024 * 1024 * 10];
         byte[] baForStream = new byte[1024];

         controller.stop(SERVER_2_NAME);

         // Server memory consumption data are also gathered, but do not
         // influence result
         MBeanServerConnectionProvider provider = new MBeanServerConnectionProvider(
               server1.getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);

         RemoteCache<String, byte[]> cache = rcm1.getCache("streamingTestCache");

         CompositeDataSupport attribute = (CompositeDataSupport) provider.getConnection()
               .getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage");

         Runtime runtime = Runtime.getRuntime();

         Long averageMemoryConsumptionStatistic = new Long(0);
         Long totalMemoryConsumptionStatistic = new Long(0);

         // test is ran 10 times to get reasonable result pool
         for (int top = 0; top < 10; top++) {

            System.gc();
            MemoryUsage serverMem = new MemoryUsage((long) attribute.get(USED_MEMORY_KEY));
            MemoryUsage clientMem = new MemoryUsage(runtime.totalMemory() - runtime.freeMemory());

            for (int i = 0; i < 10; i++) {
               Long key = random.nextLong();
               random.nextBytes(ba);
               cache.put(key.toString(), ba);

               serverMem.update((Long) ((CompositeDataSupport) provider.getConnection()
                     .getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage")).get(USED_MEMORY_KEY));
               clientMem.update(runtime.totalMemory() - runtime.freeMemory());

               System.gc();
               cache.remove(key.toString());
            }

            System.gc();
            MemoryUsage serverMemStreaming = new MemoryUsage((long) attribute.get(USED_MEMORY_KEY));
            MemoryUsage clientMemStreaming = new MemoryUsage(runtime.totalMemory() - runtime.freeMemory());

            StreamingRemoteCache<String> streamingCache = cache.streaming();

            for (int i = 0; i < 10; i++) {
               Long key = random.nextLong();
               OutputStream out = streamingCache.put(key.toString());
               for (int y = 0; y < 1024 * 10; y++) {
                  random.nextBytes(baForStream);
                  out.write(baForStream);
               }
               out.close();

               serverMemStreaming.update((Long) ((CompositeDataSupport) provider.getConnection()
                     .getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage")).get(USED_MEMORY_KEY));
               clientMemStreaming.update(runtime.totalMemory() - runtime.freeMemory());
               System.gc();
               cache.remove(key.toString());
            }

            Long averageMemoryConsumptionDifference = (clientMemStreaming.getAverage() - clientMemStreaming.getMin())
                  / ((clientMem.getAverage() - clientMem.getMin()) / 100);

            Long totalMemoryConsumptionDifference = (clientMemStreaming.getMax() - clientMemStreaming.getMin())
                  / ((clientMem.getMax() - clientMem.getMin()) / 100);

            if (averageMemoryConsumptionStatistic == 0)
               averageMemoryConsumptionStatistic = averageMemoryConsumptionDifference;
            else
               averageMemoryConsumptionStatistic = (averageMemoryConsumptionStatistic
                     + averageMemoryConsumptionDifference) / 2;

            if (totalMemoryConsumptionStatistic == 0)
               totalMemoryConsumptionStatistic = totalMemoryConsumptionDifference;
            else
               totalMemoryConsumptionStatistic = (totalMemoryConsumptionStatistic + totalMemoryConsumptionDifference)
                     / 2;
         }

         assertTrue("Average memory consumption difference outside limit, max 15, actual "
               + averageMemoryConsumptionStatistic, averageMemoryConsumptionStatistic < 15);
         assertTrue(
               "Total memory consumption difference outside limit, max 30, actual " + totalMemoryConsumptionStatistic,
               totalMemoryConsumptionStatistic < 30);
      } finally {
         checkServers();
      }
   }

   private boolean isFinalized() {
      return finalized;
   }

   private void setFinalized(boolean finalized) {
      this.finalized = finalized;
   }

   /**
    * Convenience method for killing servers according to number parity
    *
    * @param i
    */
   private void killServer(int i) {
      if (i % 2 == 0) {
         controller.kill(SERVER_1_NAME);
      } else {
         controller.kill(SERVER_2_NAME);
      }
      rcm1 = null;
      rcm2 = null;
   }

   /**
    * Convenience method for stopping servers according to number parity
    *
    * @param i
    */
   private void stopServer(int i) {
      if (i % 2 == 0) {
         controller.stop(SERVER_1_NAME);
      } else {
         controller.stop(SERVER_2_NAME);
      }
      rcm1 = null;
      rcm2 = null;
   }

   /**
    * Convenience method for starting servers according to number parity
    *
    * @param i
    */
   private void startServer(int i) {
      if (i % 2 == 0) {
         controller.start(SERVER_1_NAME);
      } else {
         controller.start(SERVER_2_NAME);
      }
      setUp();
   }

   /**
    * Testing class that will put object via stream into a cache (random key and
    * size) and then retrieve it and check for consistency
    *
    * @author zhostasa
    *
    */
   private class RandomInserter {

      private Long seed;
      private int size = random.nextInt(1000000);
      private Random randForData;
      private StreamingRemoteCache<Object> cache1;
      private StreamingRemoteCache<Object> cache2;
      private OutputStream outStream;
      private InputStream inStream;

      private int count = 0;

      private Boolean state;

      /**
       * Both cache instances can be identical
       *
       * @param seed
       *           seed for object data, can be null
       * @param cache1
       *           cache to put object to
       * @param cache2
       *           cache to get object from
       */
      public RandomInserter(Long seed, StreamingRemoteCache<Object> cache1, StreamingRemoteCache<Object> cache2) {
         this.seed = seed != null ? seed : random.nextLong();
         this.cache1 = cache1;
         this.cache2 = cache2;
         randForData = new Random(this.seed);
      }

      /**
       *
       * @return true if the test was finished successfully, false otherwise
       * @throws Exception
       *            Exception is to be considered a failed test
       */
      public boolean process() throws Exception {
         if (count == size && state == false)
            return true;
         else {
            int randomInt = random.nextInt(100);

            if (state == null) {
               state = true;
               outStream = cache1.put(seed);
            }

            if (state) {
               if (randomInt + count > size)
                  randomInt = size - count;
               byte[] arr = new byte[randomInt];
               randomBytes(arr, arr.length);
               outStream.write(arr);
               count = count + arr.length;

            } else {
               byte[] arr = new byte[randomInt];
               int ret = inStream.read(arr);
               byte[] fromrand = new byte[arr.length];
               randomBytes(fromrand, ret);

               if (ret < arr.length)
                  for (int i = ret; i < fromrand.length; i++)
                     fromrand[i] = 0;

               if (!Arrays.equals(arr, fromrand)) {
                  throw new Exception("Data returned from stream were not correct");
               }
               count = count + ret;

            }

            if (count == size) {
               if (state) {
                  state = false;
                  outStream.close();
                  inStream = cache2.get(seed);
                  randForData = new Random(seed);
                  count = 0;
               } else {
                  inStream.close();
                  return true;
               }
            }
         }
         return false;

      }

      /**
       * Sets finalized flag in parent class for GC test
       *
       * @throws Throwable
       */
      @Override
      public void finalize() throws Throwable {
         super.finalize();
         setFinalized(true);
      }

      /**
       * Because Random.nextBytes(byte[]) will drop bytes between calls
       *
       * @param ba
       *           byte[] to fill
       * @param count
       *           number or bytes to fill in
       */
      private void randomBytes(byte[] ba, int count) {
         for (int i = 0; i < count; i++) {
            ba[i] = (byte) randForData.nextInt();
         }
      }
   }

   /**
    * Simple class for computing memory usage
    *
    * @author zhostasa
    *
    */
   private class MemoryUsage {

      private Long min, max, average;

      public MemoryUsage(Long startValue) {
         this(startValue, startValue, startValue);
      }

      public MemoryUsage(Long min, Long max, Long average) {
         this.min = min;
         this.max = max;
         this.average = average;
      }

      public void update(Long value) {
         setMax(value);
         setMin(value);
         addToAverage(value);
      }

      public Long getMax() {
         return max;
      }

      private void setMax(Long max) {
         if (this.max < max)
            this.max = max;
      }

      public Long getMin() {
         return min;
      }

      private void setMin(Long min) {
         if (this.min > min)
            this.min = min;
      }

      public Long getAverage() {
         return average;
      }

      private void addToAverage(Long average) {
         this.average = (this.average + average) / 2;
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         sb.append("MemoryStats:\n");
         sb.append("Max memory: " + getMax() + "\n");
         sb.append("Min memory: " + getMin() + "\n");
         sb.append("Avg memory: " + getAverage() + "\n");
         return sb.toString();
      }

   }
}
