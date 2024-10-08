package org.infinispan.server.resp.dist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.server.resp.TransactionOperationsTest;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.annotations.Test;

import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "dist.server.resp.TransactionClusteredTest")
public class TransactionClusteredTest extends TransactionOperationsTest {

   private CacheMode mode;

   private TransactionClusteredTest withCacheMode(CacheMode mode) {
      this.mode = mode;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new TransactionClusteredTest().withCacheMode(CacheMode.DIST_SYNC),
            new TransactionClusteredTest().withCacheMode(CacheMode.REPL_SYNC),
      };
   }

   private String getStringKeyForCache(String prefix, Cache<?, ?> primary, Cache<?, ?> ... secondary) {
      LocalizedCacheTopology topology = primary.getAdvancedCache().getDistributionManager().getCacheTopology();
      Predicate<String> isBackupOwner = key -> Arrays.stream(secondary)
            .allMatch(c -> {
               LocalizedCacheTopology t = c.getAdvancedCache().getDistributionManager().getCacheTopology();
               int segment = t.getSegment(new WrappedByteArray(key.getBytes(StandardCharsets.US_ASCII)));
               return t.isSegmentReadOwner(segment);
            });
      return IntStream.generate(ThreadLocalRandom.current()::nextInt).mapToObj(i -> prefix + i)
            .filter(key -> topology.getDistribution(new WrappedByteArray(key.getBytes(StandardCharsets.US_ASCII))).isPrimary())
            .filter(isBackupOwner).findAny().orElseThrow();
   }

   private String getRemoteKey() {
      return getStringKeyForCache("key", cache(1), cache(2));
   }

   @Override
   public void testBlockingPopWithTx() throws Throwable {
      testBlockingPopWithTx(this::getRemoteKey);
   }

   public void testFunctionalTxWithRemote() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.multi()).isEqualTo("OK");
      assertThat(redisConnection.isMulti()).isTrue();

      String k1 = getRemoteKey();
      redis.lpush(k1, "v1", "v2", "v3");

      String k2 = getRemoteKey();
      redis.hset(k2, Map.of("f1", "v1", "f2", "v2"));

      String k3 = getRemoteKey();
      redis.set(k3, "value");

      TransactionResult result = redis.exec();
      assertThat(result.wasDiscarded()).isFalse();
      assertThat((Object) result.get(0))
            .isInstanceOfSatisfying(Long.class, v -> assertThat(v).isEqualTo(3L));

      assertThat((Object) result.get(1))
            .isInstanceOfSatisfying(Long.class, v -> assertThat(v).isEqualTo(2L));

      assertThat((Object) result.get(2)).isEqualTo(OK);
   }

   @Override
   protected String parameters() {
      return "[mode=" + mode + "]";
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      super.amendConfiguration(configurationBuilder);
      configurationBuilder.clustering().cacheMode(mode);
   }

   @Override
   protected TestSetup setup() {
      return TestSetup.clusteredTestSetup(3);
   }
}
