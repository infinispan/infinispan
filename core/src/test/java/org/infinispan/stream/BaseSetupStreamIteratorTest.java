package org.infinispan.stream;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.HashConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.ScatteredConsistentHash;
import org.infinispan.filter.Converter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
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
   protected SerializationContextInitializer sci;

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
      sci = new StreamSerializationContextImpl();
      HashConfigurationBuilder hashConfiguration = builderUsed.clustering().cacheMode(cacheMode).hash().numSegments(3);
      if (!cacheMode.isReplicated()) {
         BaseControlledConsistentHashFactory<? extends ConsistentHash> chf =
               cacheMode.isScattered() ? new TestScatteredConsistentHashFactory() : new TestDefaultConsistentHashFactory();
         hashConfiguration.consistentHashFactory(chf);
      }
      if (transactional) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      if (cacheMode.isClustered()) {
         builderUsed.clustering().stateTransfer().chunkSize(50);
         enhanceConfiguration(builderUsed);
         createClusteredCaches(3, CACHE_NAME, sci, builderUsed);
      } else {
         enhanceConfiguration(builderUsed);
         EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(sci, builderUsed);
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

   @ProtoName("BaseSetupStreamStringTrunctator")
   public static class StringTruncator implements Converter<Object, String, String> {
      @ProtoField(number = 1, defaultValue = "0")
      final int beginning;

      @ProtoField(number = 2, defaultValue = "0")
      final int length;

      @ProtoFactory
      StringTruncator(int beginning, int length) {
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

   public static class TestDefaultConsistentHashFactory
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

   public static class TestScatteredConsistentHashFactory
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

   protected <C> C replaceComponentWithSpy(Cache<?,?> cache, Class<C> componentClass) {
      C component = TestingUtil.extractComponent(cache, componentClass);
      C spiedComponent = spy(component);
      TestingUtil.replaceComponent(cache, componentClass, spiedComponent, true);
      reset(spiedComponent);
      return spiedComponent;
   }

   protected Map<Integer, Set<Map.Entry<Object, String>>> generateEntriesPerSegment(KeyPartitioner keyPartitioner,
                                                                                  Iterable<Map.Entry<Object, String>> entries) {
      Map<Integer, Set<Map.Entry<Object, String>>> returnMap = new HashMap<>();

      for (Map.Entry<Object, String> value : entries) {
         int segment = keyPartitioner.getSegment(value.getKey());
         Set<Map.Entry<Object, String>> set = returnMap.computeIfAbsent(segment, k -> new LinkedHashSet<>());
         set.add(new ImmortalCacheEntry(value.getKey(), value.getValue()));
      }
      return returnMap;
   }

   @AutoProtoSchemaBuilder(
         // TODO use this or just explicitly add required classes?
//         dependsOn = org.infinispan.test.TestDataSCI.class,
         includeClasses = {
               BaseSetupStreamIteratorTest.StringTruncator.class,
               BaseSetupStreamIteratorTest.TestDefaultConsistentHashFactory.class,
               BaseSetupStreamIteratorTest.TestScatteredConsistentHashFactory.class,
               MagicKey.class
         },
         schemaFileName = "core.stream.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.stream")
   interface StreamSerializationContext extends SerializationContextInitializer {
   }
}
