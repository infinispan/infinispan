package org.infinispan.distribution.ch;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.testng.AssertJUnit.assertEquals;

import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.2
 */
@Test(groups = "functional", testName = "distribution.ch.AffinityPartitionerTest")
public class AffinityPartitionerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      addNodes(2);
   }

   private void addNodes(int count) {
      final ConfigurationBuilder conf = getConfigurationBuilder();
      createCluster(new DistributionSerializationContextImpl(), conf, count);
      waitForClusterToForm();
   }

   @Test
   public void testAffinityPartitioner() throws Exception {
      Cache<AffinityKey, String> cache = cacheManagers.get(0).getCache();
      IntStream.range(0, 10).boxed().forEach(num -> cache.put(new AffinityKey(num), "value"));
      addNodes(1);

      cacheManagers.stream().map(cm -> cm.getCache().getAdvancedCache()).forEach(advancedCache -> {
         LocalizedCacheTopology cacheTopology = advancedCache.getDistributionManager().getCacheTopology();
         advancedCache.getDataContainer().forEach(ice -> {
            Object key = ice.getKey();
            int keySegmentId = ((AffinityKey) key).segmentId;
            assertEquals(cacheTopology.getSegment(key), keySegmentId);
         });
      });
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      final ConfigurationBuilder conf = getDefaultClusteredCacheConfig(DIST_SYNC, false);
      conf.clustering().hash().keyPartitioner(new AffinityPartitioner()).numSegments(10).numOwners(1);
      return conf;
   }

   public static class AffinityKey implements AffinityTaggedKey {

      @ProtoField(number = 1, defaultValue = "0")
      final int segmentId;

      @ProtoFactory
      AffinityKey(int segmentId) {
         this.segmentId = segmentId;
      }

      @Override
      public int getAffinitySegmentId() {
         return segmentId;
      }
   }

   @AutoProtoSchemaBuilder(
         includeClasses = AffinityKey.class,
         schemaFileName = "core.distribution.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.distribution")
   interface DistributionSerializationContext extends SerializationContextInitializer {
   }
}
