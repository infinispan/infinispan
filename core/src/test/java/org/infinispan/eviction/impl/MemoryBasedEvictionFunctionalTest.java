package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.SerializationConfigurationBuilder;
import org.infinispan.container.offheap.OffHeapConcurrentMap;
import org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.MemoryBasedEvictionFunctionalTest")
public class MemoryBasedEvictionFunctionalTest extends SingleCacheManagerTest {

   protected static final long CACHE_SIZE = 2000;

   protected StorageType storageType;

   public MemoryBasedEvictionFunctionalTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   protected void configure(ConfigurationBuilder cb) { }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.memory().storage(storageType);
      builder.memory().maxSize(CACHE_SIZE + UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(OffHeapConcurrentMap.INITIAL_SIZE << 3));

      configure(builder);

      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      SerializationConfigurationBuilder serialization = globalBuilder.serialization();
      serialization.addContextInitializer(TestDataSCI.INSTANCE);

      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(globalBuilder, builder);
      cache = cm.getCache();
      return cm;
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new MemoryBasedEvictionFunctionalTest().storageType(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected String parameters() {
      return "[storageType=" + storageType + "]";
   }

   public void testByteArray() throws Exception {
      int keyValueByteSize = 100;
      long numberInserted = CACHE_SIZE / 2 / keyValueByteSize;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (long i = 0; i < numberInserted; i++) {
         byte[] key = new byte[keyValueByteSize];
         byte[] value = new byte[keyValueByteSize];
         random.nextBytes(key);
         random.nextBytes(value);
         cache.put(key, value);
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testByteObjectArray() throws Exception {
      int keyValueByteSize = 100;
      long numberInserted = CACHE_SIZE / 2 / keyValueByteSize;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (long i = 0; i < numberInserted; i++) {
         byte[] key = new byte[keyValueByteSize];
         Byte[] value = new Byte[keyValueByteSize];
         fillByteArray(random, key);
         fillByteArray(random, value);
         cache.put(key, value);
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   private void fillByteArray(Random random, byte[] bytes) {
      random.nextBytes(bytes);
   }

   private void fillByteArray(Random random, Byte[] bytes) {
      byte[] singleByte = new byte[1];
      for (int i = 0; i < bytes.length; ++i) {
         random.nextBytes(singleByte);
         bytes[i] = Byte.valueOf(singleByte[0]);
      }
   }

   public void testShort() throws Exception {
      long numberInserted = CACHE_SIZE / 2;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (short i = 0; i < numberInserted; i++) {
         cache.put(i, Short.valueOf((short) random.nextInt(Short.MAX_VALUE + 1)));
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testShortArray() throws Exception {
      int arraySize = 10;
      long numberInserted = CACHE_SIZE / 2 / arraySize;
      Random random = new Random();
      short[] shortArray = new short[arraySize];
      Short[] ShortArray = new Short[arraySize];
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (short i = 0; i < numberInserted; i++) {
         IntStream.range(0, arraySize).forEach(j -> shortArray[j] = (short) random.nextInt(Short.MAX_VALUE));
         Arrays.setAll(ShortArray, j -> Short.valueOf((short) random.nextInt(Short.MAX_VALUE)));
         cache.put(shortArray, ShortArray);
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testInteger() throws Exception {
      long numberInserted = CACHE_SIZE / 8;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (int i = 0; i < numberInserted; i++) {
         cache.put(i, Integer.valueOf(random.nextInt()));
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testIntegerArray() throws Exception {
      int arraySize = 10;
      long numberInserted = CACHE_SIZE / 4 / arraySize;
      Random random = new Random();
      int[] integerArray = new int[arraySize];
      Integer[] IntegerArray = new Integer[arraySize];
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (short i = 0; i < numberInserted; i++) {
         Arrays.setAll(integerArray, j -> random.nextInt());
         Arrays.setAll(IntegerArray, j -> Integer.valueOf(random.nextInt()));
         cache.put(integerArray, IntegerArray);
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testLong() throws Exception {
      long numberInserted = CACHE_SIZE / 4;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (long i = 0; i < numberInserted; i++) {
         cache.put(i, Long.valueOf(random.nextLong()));
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testLongArray() throws Exception {
      int arraySize = 10;
      long numberInserted = CACHE_SIZE / 8 / arraySize;
      Random random = new Random();
      long[] longArray = new long[arraySize];
      Long[] LongArray = new Long[arraySize];
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (short i = 0; i < numberInserted; i++) {
         Arrays.setAll(longArray, j -> random.nextLong());
         Arrays.setAll(LongArray, j -> Long.valueOf(random.nextLong()));
         cache.put(longArray, LongArray);
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testByte() throws Exception {
      long numberInserted = CACHE_SIZE / 2;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      byte[] bytes = new byte[1];
      for (short i = 0; i < numberInserted; i++) {
         random.nextBytes(bytes);
         cache.put(i, bytes[0]);
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testByteObject() throws Exception {
      long numberInserted = CACHE_SIZE / 2;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      byte[] bytes = new byte[1];
      for (short i = 0; i < numberInserted; i++) {
         random.nextBytes(bytes);
         cache.put(i, Byte.valueOf(bytes[0]));
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testFloat() throws Exception {
      long numberInserted = CACHE_SIZE / 4;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (float i = 0; i < numberInserted; i++) {
         cache.put(i, Float.valueOf(random.nextFloat()));
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testDouble() throws Exception {
      long numberInserted = CACHE_SIZE / 8;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (double i = 0; i < numberInserted; i++) {
         cache.put(i, Double.valueOf(random.nextDouble()));
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testDoubleArray() throws Exception {
      int arraySize = 10;
      long numberInserted = CACHE_SIZE / 8 / arraySize;
      Random random = new Random();
      double[] doubleArray = new double[arraySize];
      Double[] DoubleArray = new Double[arraySize];
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (short i = 0; i < numberInserted; i++) {
         Arrays.setAll(doubleArray, j -> random.nextDouble());
         Arrays.setAll(DoubleArray, j -> Double.valueOf(random.nextDouble()));
         cache.put(doubleArray, DoubleArray);
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testString() throws Exception {
      int stringLength = 10;
      long numberInserted = CACHE_SIZE / stringLength + 4;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (long i = 0; i < numberInserted; i++) {
         cache.put(i, randomStringFullOfInt(random, stringLength));
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   public void testStringArray() throws Exception {
      int arraySize = 10;
      int stringLength = 10;
      long numberInserted = CACHE_SIZE / stringLength + 4 / arraySize;
      Random random = new Random();
      String[] stringArray = new String[arraySize];
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      AtomicInteger atomicInteger = new AtomicInteger();
      for (long i = 0; i < numberInserted; i++) {
         atomicInteger.set(0);
         Arrays.setAll(stringArray, j -> randomStringFullOfInt(random, stringLength));
         cache.put(i, stringArray);
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }

   protected String randomStringFullOfInt(Random random, int digits) {
      return random.ints(digits, 0, 10).collect(StringBuilder::new, StringBuilder::append,
              StringBuilder::append).toString();
   }
}
