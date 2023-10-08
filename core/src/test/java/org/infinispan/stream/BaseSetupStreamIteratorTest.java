package org.infinispan.stream;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.infinispan.filter.CompositeKeyValueFilterConverter;
import org.infinispan.filter.Converter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.GlobalContextInitializer;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.BaseControlledConsistentHashFactory;
import org.infinispan.util.KeyValuePair;
import org.testng.annotations.Test;

/**
 * Base class used solely for setting up cluster configuration for use with stream iterators
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.BaseSetupStreamIteratorTest")
public abstract class BaseSetupStreamIteratorTest extends MultipleCacheManagersTest {
   public static final int NUM_NODES = 3;
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
         BaseControlledConsistentHashFactory<? extends ConsistentHash> chf = new TestDefaultConsistentHashFactory();
         hashConfiguration.consistentHashFactory(chf);
      }
      if (transactional) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      if (cacheMode.isClustered()) {
         builderUsed.clustering().stateTransfer().chunkSize(5);
         enhanceConfiguration(builderUsed);
         createClusteredCaches(NUM_NODES, CACHE_NAME, sci, builderUsed, new TransportFlags().withFD(true));
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

   @ProtoAdapter(HashMap.class)
   public static class HashMapAdapter<K, V> {
      @ProtoFactory
      static <K, V> HashMap<K, V> create(List<KeyValuePair<K, V>> pairs) {
         HashMap<K, V> map = new HashMap<>(pairs.size());
         for (var kvp : pairs)
            map.put(kvp.getKey(), kvp.getValue());
         return map;
      }

      @ProtoField(1)
      List<KeyValuePair<K, V>> getPairs(HashMap<K, V> map) {
         return map.entrySet()
               .stream()
               .map(e -> new KeyValuePair<>(e.getKey(), e.getValue()))
               .collect(Collectors.toList());
      }
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
      protected int[][] assignOwners(int numSegments, List<Address> members) {
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

   @ProtoSchema(
         dependsOn = GlobalContextInitializer.class,
         includeClasses = {
               BaseSetupStreamIteratorTest.HashMapAdapter.class,
               BaseSetupStreamIteratorTest.StringTruncator.class,
               BaseSetupStreamIteratorTest.TestDefaultConsistentHashFactory.class,
               CompositeKeyValueFilterConverter.class,
               MagicKey.class
         },
         schemaFileName = "core.stream.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.stream",
         service = false
   )
   interface StreamSerializationContext extends SerializationContextInitializer {
   }
}
