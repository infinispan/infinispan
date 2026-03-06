package org.infinispan.statetransfer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Tests state transfer with different MediaType configurations to ensure values
 * are stored in the correct format in the DataContainer.
 */
@Test(testName = "statetransfer.StateTransferMediaTest", groups = "functional")
public class StateTransferMediaTypeTest extends MultipleCacheManagersTest {

   private MediaType keyMediaType;
   private MediaType valueMediaType;

   public StateTransferMediaTypeTest() {
      // Default constructor for factory
   }

   public StateTransferMediaTypeTest keyMediaType(MediaType keyMediaType) {
      this.keyMediaType = keyMediaType;
      return this;
   }

   public StateTransferMediaTypeTest valueMediaType(MediaType valueMediaType) {
      this.valueMediaType = valueMediaType;
      return this;
   }

   @Override
   protected String parameters() {
      return "[keyMediaType=" + keyMediaType + ", valueMediaType=" + valueMediaType + "]";
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new StateTransferMediaTypeTest()
                  .keyMediaType(MediaType.APPLICATION_OBJECT)
                  .valueMediaType(MediaType.APPLICATION_OBJECT),
            new StateTransferMediaTypeTest()
                  .keyMediaType(MediaType.APPLICATION_OCTET_STREAM)
                  .valueMediaType(MediaType.APPLICATION_OCTET_STREAM),
            new StateTransferMediaTypeTest()
                  .keyMediaType(MediaType.APPLICATION_PROTOSTREAM)
                  .valueMediaType(MediaType.APPLICATION_PROTOSTREAM),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2).numSegments(4);
      builder.encoding().key().mediaType(keyMediaType.toString());
      builder.encoding().value().mediaType(valueMediaType.toString());

      createCluster(TestDataSCI.INSTANCE, builder, 1);
      waitForClusterToForm();
   }

   @Test
   public void testStateTransferWithMediaType() {
      Cache<String, String> cache0 = cache(0);

      // Write 20 entries into the cache
      for (int i = 0; i < 20; i++) {
         cache0.put("key" + i, "value" + i);
      }

      // Add 3 nodes
      addClusterEnabledCacheManager(TestDataSCI.INSTANCE, builder());
      addClusterEnabledCacheManager(TestDataSCI.INSTANCE, builder());
      addClusterEnabledCacheManager(TestDataSCI.INSTANCE, builder());

      waitForClusterToForm();

      Cache<String, String> cache1 = cache(1);
      Cache<String, String> cache2 = cache(2);
      Cache<String, String> cache3 = cache(3);

      // Wait for state transfer to complete
      TestingUtil.waitForNoRebalance(cache0, cache1, cache2, cache3);

      // Check all nodes to ensure values are in the proper storage format
      verifyStorageFormat(cache0);
      verifyStorageFormat(cache1);
      verifyStorageFormat(cache2);
      verifyStorageFormat(cache3);

      int expectedSize = 20;
      byte[] expectedPrev = "value0".getBytes();
      if (keyMediaType != MediaType.APPLICATION_OCTET_STREAM) {
         // Use cache with octet-stream request media type and insert key0 as byte[]
         Cache<byte[], byte[]> octetStreamCache = cache0.getAdvancedCache()
               .withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM);
         octetStreamCache.put("key0".getBytes(), "value0_updated".getBytes());

         // Now use a different encoding and see if the value is replaced or not
         assertEquals("Cache entry should not be overwritten", ++expectedSize, cache0.size());
         expectedPrev = "value0_updated".getBytes();
      }

      // Use application object, but insert it as a byte[] to see what happens
      // Note for object and protostream this replaces the one inserted in the previous if
      Cache<Object, Object> objectCache = cache0.getAdvancedCache()
            .withMediaType(MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_OBJECT);
      Object prev = objectCache.put("key0".getBytes(), "value0_updated".getBytes());
      assertEquals(expectedPrev, (byte[]) prev);

      // Now use a different encoding and see if the value is replaced or not
      assertEquals("Cache entry should not be overwritten", expectedSize, cache0.size());
   }

   private ConfigurationBuilder builder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2).numSegments(4);
      builder.encoding().key().mediaType(keyMediaType.toString());
      builder.encoding().value().mediaType(valueMediaType.toString());
      return builder;
   }

   private void verifyStorageFormat(Cache<String, String> cache) {
      DataContainer<Object, Object> dataContainer = (DataContainer) cache.getAdvancedCache().getDataContainer();

      for (InternalCacheEntry<Object, Object> entry : dataContainer) {
         Object key = entry.getKey();
         Object value = entry.getValue();

         assertNotNull("Key should not be null", key);
         assertNotNull("Value should not be null", value);

         // Verify key format based on configured media type
         verifyFormat("Key", key, keyMediaType);

         // Verify value format based on configured media type
         verifyFormat("Value", value, valueMediaType);
      }
   }

   private void verifyFormat(String description, Object obj, MediaType mediaType) {
      if (mediaType.match(MediaType.APPLICATION_OBJECT)) {
         // For APPLICATION_OBJECT, the value should be the original String type
         assertTrue(description + " should be a String for APPLICATION_OBJECT, but was " + obj.getClass(),
               obj instanceof String);
      } else if (mediaType.match(MediaType.APPLICATION_OCTET_STREAM)) {
         // For APPLICATION_OCTET_STREAM, the value should be byte[] or WrappedByteArray
         assertTrue(description + " should be byte[] or WrappedByteArray for APPLICATION_OCTET_STREAM, but was " + obj.getClass(),
               obj instanceof WrappedByteArray);
      } else if (mediaType.match(MediaType.APPLICATION_PROTOSTREAM)) {
         // For APPLICATION_PROTOSTREAM, the value should be byte[] or WrappedByteArray
         assertTrue(description + " should be byte[] or WrappedByteArray for APPLICATION_PROTOSTREAM, but was " + obj.getClass(),
               obj instanceof WrappedByteArray);
      }
   }
}
