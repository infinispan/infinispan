package org.infinispan.server.functional.hotrod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.test.Combinations;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

public class HotRodFlagCacheOperations {

   private static final String TEST_OUTPUT = "{0}";

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   static final class FlagsProvider implements ArgumentsProvider {

      @Override
      public Stream<? extends Arguments> provideArguments(ParameterDeclarations parameters, ExtensionContext context) {
         return Combinations.combine(Flag.class).stream()
               .map(f -> Arguments.of(EnumSet.copyOf(f)));
      }
   }

   private <K, V> RemoteCache<K, V> remoteCache(boolean frv, EnumSet<Flag> flags) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.forceReturnValues(frv);
      return SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).<K, V>create().withFlags(flags.toArray(new Flag[0]));
   }

   private <K, V> RemoteCache<K, V> remoteCache(EnumSet<Flag> flags) {
      return remoteCache(false, flags);
   }

   private <K, V> RemoteCache<K, V> remoteQueryableCache(boolean indexed, EnumSet<Flag> flags) {
      return Common.<K, V>createQueryableCache(SERVERS, indexed, TestDomainSCI.INSTANCE, HotRodCacheQueries.ENTITY_USER)
            .withFlags(flags.toArray(new Flag[0]));
   }

   @ParameterizedTest(name = TEST_OUTPUT)
   @ArgumentsSource(FlagsProvider.class)
   public void testBasicOperations(EnumSet<Flag> flags) {
      RemoteCache<String, String> cache = remoteCache(true, flags);

      assertThat(cache.get(k())).isNull();
      assertThat(cache.put(k(), v())).isNull();
      assertThat(cache.get(k())).isEqualTo(v());
      assertThat(cache.size()).isOne();

      cache.remove(k());
      assertThat(cache.get(k())).isNull();
   }

   @ParameterizedTest(name = TEST_OUTPUT)
   @ArgumentsSource(FlagsProvider.class)
   public void testBulkOperations(EnumSet<Flag> flags) {
      RemoteCache<String, String> cache = remoteCache(flags);

      Map<String, String> content = new HashMap<>();
      for (int i = 0; i < 25; i++) {
         content.put("key-" + i, "value-" + i);
      }

      assertThat(cache.isEmpty()).isTrue();
      cache.putAll(content);

      assertThat(cache.size()).isEqualTo(content.size());
      try (CloseableIterator<Map.Entry<String, String>> it = cache.entrySet().iterator()) {
         while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            assertThat(content).containsKey(entry.getKey());
            assertThat(content.get(entry.getKey())).isEqualTo(entry.getValue());
            content.remove(entry.getKey());
         }
      }

      assertThat(content).isEmpty();
      cache.clear();
   }

   @ParameterizedTest(name = TEST_OUTPUT)
   @ArgumentsSource(FlagsProvider.class)
   public void testConditionalOperations(EnumSet<Flag> flags) {
      RemoteCache<String, String> cache = remoteCache(flags);

      assertThat(cache.get(k())).isNull();

      ByRef.Integer invocations = new ByRef.Integer(0);
      cache.computeIfPresent(k(), (k, v) -> {
         invocations.inc();
         return "new-value";
      });

      assertThat(invocations.get()).isZero();

      String value = "value";
      cache.computeIfAbsent(k(), k -> {
         invocations.inc();
         return value;
      });

      assertThat(invocations.get()).isOne();

      String updatedValue = "updated-value";
      cache.compute(k(), (k, v) -> {
         invocations.inc();
         assertThat(v).isEqualTo(value);
         return updatedValue;
      });

      assertThat(invocations.get()).isEqualTo(2);
      assertThat(cache.get(k())).isEqualTo(updatedValue);

      cache.computeIfPresent(k(), (k, v) -> {
         invocations.inc();
         assertThat(v).isEqualTo(updatedValue);
         return null;
      });

      assertThat(invocations.get()).isEqualTo(3);
      assertThat(cache.get(k())).isNull();
   }

   @ParameterizedTest(name = TEST_OUTPUT)
   @ArgumentsSource(FlagsProvider.class)
   public void testMoreConditionalOperations(EnumSet<Flag> flags) {
      RemoteCache<String, String> cache = remoteCache(true, flags);

      assertThat(cache.get(k())).isNull();
      assertThat(cache.putIfAbsent(k(), v())).isNull();

      assertThat(cache.get(k())).isEqualTo(v());

      String newValue = "new-value";
      assertThat(cache.replace(k(), v(), newValue)).isTrue();

      assertThat(cache.get(k())).isEqualTo(newValue);
      assertThat(cache.replace(k(), "something", "else")).isFalse();
      assertThat(cache.get(k())).isEqualTo(newValue);

      assertThat(cache.remove(k(), "something-else")).isFalse();
      assertThat(cache.remove(k(), newValue)).isTrue();
   }

   @ParameterizedTest(name = TEST_OUTPUT)
   @ArgumentsSource(FlagsProvider.class)
   public void testPublishers(EnumSet<Flag> flags) throws Throwable {
      RemoteCache<String, String> cache = remoteCache(flags);

      Map<String, String> content = new HashMap<>();
      for (int i = 0; i < 25; i++) {
         content.put("key-" + i, "value-" + i);
      }

      assertThat(cache.isEmpty()).isTrue();
      cache.putAll(content);

      CompletableFuture<Void> consumer = new CompletableFuture<>();
      Publisher<Map.Entry<String, String>> publisher = cache.publishEntries(null, null, null, 5);
      Flowable.fromPublisher(publisher)
            .subscribe(e -> {
               assertThat(content).containsKey(e.getKey());
               assertThat(content.get(e.getKey())).isEqualTo(e.getValue());
               content.remove(e.getKey());
            }, consumer::completeExceptionally, () -> consumer.complete(null));

      consumer.get(15, TimeUnit.SECONDS);
      assertThat(content).isEmpty();
      cache.clear();
   }

   @ParameterizedTest(name = TEST_OUTPUT)
   @ArgumentsSource(FlagsProvider.class)
   public void testKeyPublisher(EnumSet<Flag> flags) {
      RemoteCache<String, String> cache = remoteCache(flags);

      Map<String, String> content = new HashMap<>();
      for (int i = 0; i < 25; i++) {
         content.put("key-" + i, "value-" + i);
      }

      assertThat(cache.isEmpty()).isTrue();
      cache.putAll(content);

      CloseableIteratorSet<String> keys = cache.keySet();
      for (String key : keys) {
         assertThat(content.remove(key)).isNotNull();
      }
      assertThat(content).isEmpty();
      cache.clear();
   }

   @ParameterizedTest(name = TEST_OUTPUT)
   @ArgumentsSource(FlagsProvider.class)
   public void testQueryableCacheIndexed(EnumSet<Flag> flags) {
      testQueryCache(true, flags);
   }

   @ParameterizedTest(name = TEST_OUTPUT)
   @ArgumentsSource(FlagsProvider.class)
   public void testQueryableCacheNotIndexed(EnumSet<Flag> flags) {
      testQueryCache(false, flags);
   }

   private void testQueryCache(boolean indexed, EnumSet<Flag> flags) {
      RemoteCache<Integer, User> cache = remoteQueryableCache(indexed, flags);
      cache.put(1, HotRodCacheQueries.createUser1());

      User u = cache.get(1);
      HotRodCacheQueries.assertUser1(u);

      Query<User> query = cache.query("FROM sample_bank_account.User WHERE name = 'Tom'");
      List<User> users = query.execute().list();

      // An indexed cache that skips indexing won't have the entity returned from the query.
      if (indexed && flags.contains(Flag.SKIP_INDEXING)) {
         assertThat(users).isNotNull().isEmpty();
         return;
      }

      assertThat(users)
            .isNotNull()
            .hasSize(1);
      HotRodCacheQueries.assertUser1(users.get(0));
   }
}
