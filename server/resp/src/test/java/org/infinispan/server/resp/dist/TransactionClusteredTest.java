package org.infinispan.server.resp.dist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;

import java.util.Map;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.TransactionOperationsTest;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.annotations.Test;

import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "dist.server.resp.TransactionClusteredTest")
public class TransactionClusteredTest extends TransactionOperationsTest {

   private CacheMode mode;
   private boolean fromOwner;

   private TransactionClusteredTest withCacheMode(CacheMode mode) {
      this.mode = mode;
      return this;
   }

   private TransactionClusteredTest fromOwner(boolean value) {
      this.fromOwner = value;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new TransactionClusteredTest().withCacheMode(CacheMode.DIST_SYNC).fromOwner(false),
            new TransactionClusteredTest().withCacheMode(CacheMode.DIST_SYNC).fromOwner(true),
            new TransactionClusteredTest().withCacheMode(CacheMode.REPL_SYNC),
      };
   }

   @Override
   protected String getOperationKey(int i) {
      return fromOwner
            ? getStringKeyForCache("key-" + i, cache(0))
            : getStringKeyForCache("key-" + i, cache(1));
   }

   public void testFunctionalTxWithRemote() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.multi()).isEqualTo("OK");
      assertThat(redisConnection.isMulti()).isTrue();

      String k1 = getOperationKey(0);
      redis.lpush(k1, "v1", "v2", "v3");

      String k2 = getOperationKey(1);
      redis.hset(k2, Map.of("f1", "v1", "f2", "v2"));

      String k3 = getOperationKey(2);
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
      return "[mode=" + mode + ", fromOwner=" + fromOwner + "]";
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      super.amendConfiguration(configurationBuilder);
      configurationBuilder.clustering().hash().numOwners(1);
      configurationBuilder.clustering().cacheMode(mode);
   }

   @Override
   protected TestSetup setup() {
      return TestSetup.clusteredTestSetup(3);
   }
}
