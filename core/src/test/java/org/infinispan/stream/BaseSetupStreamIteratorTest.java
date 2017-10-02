package org.infinispan.stream;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.ScatteredConsistentHash;
import org.infinispan.filter.Converter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.testng.annotations.Test;

/**
 * Base class used solely for setting up cluster configuration for use with stream iterators
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.BaseSetupStreamIteratorTest")
public abstract class BaseSetupStreamIteratorTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = "testCache";
   protected ConfigurationBuilder builderUsed;

   public BaseSetupStreamIteratorTest(boolean tx, CacheMode mode) {
      transactional = tx;
      cacheMode = mode;
   }

   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      // Do nothing to config by default, used by people who extend this
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      BaseControlledConsistentHashFactory<? extends ConsistentHash> chf =
            cacheMode.isScattered() ? new TestScatteredConsistentHashFactory() : new TestDefaultConsistentHashFactory();
      builderUsed.clustering().cacheMode(cacheMode).hash().numSegments(3).consistentHashFactory(chf);
      if (transactional) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      if (cacheMode.isClustered()) {
         builderUsed.clustering().stateTransfer().chunkSize(50);
         enhanceConfiguration(builderUsed);
         createClusteredCaches(3, CACHE_NAME, builderUsed);
      } else {
         enhanceConfiguration(builderUsed);
         EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builderUsed);
         cacheManagers.add(cm);
         cm.defineConfiguration(CACHE_NAME, builderUsed.build());
      }
   }

   protected static <K, V> Map<K, V> mapFromIterator(Iterator<? extends Map.Entry<K, V>> iterator) {
      Map<K, V> map = new HashMap<>();
      while (iterator.hasNext()) {
         Map.Entry<K, V> entry = iterator.next();
         map.put(entry.getKey(), entry.getValue());
      }
      return map;
   }

   protected static <K, V> Map<K, V> mapFromStream(CacheStream<CacheEntry<K, V>> stream) {
      return stream.collect(() -> Collectors.toMap(CacheEntry::getKey, CacheEntry::getValue));
   }

   protected static class StringTruncator implements Converter<Object, String, String>, Serializable, ExternalPojo {
      private final int beginning;
      private final int length;

      public StringTruncator(int beginning, int length) {
         this.beginning = beginning;
         this.length = length;
      }

      @Override
      public String convert(Object key, String value, Metadata metadata) {
         if (value != null && value.length() > beginning + length) {
            return value.substring(beginning, beginning + length);
         } else {
            throw new IllegalStateException("String should be longer than truncation size!  Possible double conversion performed!");
         }
      }
   }

   private static class TestDefaultConsistentHashFactory
         extends BaseControlledConsistentHashFactory<DefaultConsistentHash> {
      TestDefaultConsistentHashFactory() {
         super(new DefaultTrait(), 3);
      }

      @Override
      protected int[][] assignOwners(int numSegments, int numOwners, List<Address> members) {
         // The test needs a segment owned by nodes 01, 12, and 21 when there are 3 nodes in the cluster.
         // There are no restrictions for before/after, so we make the coordinator the primary owner of all segments.
         switch (members.size()) {
            case 1:
               return new int[][]{{0}, {0}, {0}};
            case 2:
               return new int[][]{{0, 0}, {0, 1}, {0, 1}};
            default:
               return new int[][]{{0, 1}, {1, 2}, {2, 1}};
         }
      }
   }

   private static class TestScatteredConsistentHashFactory
         extends BaseControlledConsistentHashFactory<ScatteredConsistentHash> {
      TestScatteredConsistentHashFactory() {
         super(new ScatteredTrait(), 3);
      }

      @Override
      protected int[][] assignOwners(int numSegments, int numOwners, List<Address> members) {
         // The test needs a segment owned by each node when there are 3 nodes in the cluster.
         // There are no restrictions for before/after, so we make the coordinator the primary owner of all segments.
         switch (members.size()) {
            case 1:
               return new int[][]{{0}, {0}, {0}};
            case 2:
               return new int[][]{{0}, {0}, {0}};
            default:
               return new int[][]{{0}, {1}, {2}};
         }
      }
   }
}
