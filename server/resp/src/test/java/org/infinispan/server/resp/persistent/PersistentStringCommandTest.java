package org.infinispan.server.resp.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.SingleNodeRespBaseTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisCommands;

@CleanupAfterMethod
@Test(groups = "functional", testName = "server.resp.persistent.PersistentStringCommandTest")
public class PersistentStringCommandTest extends SingleNodeRespBaseTest {

   @AfterClass(alwaysRun = true)
   protected void removeData() {
      Util.recursiveFileRemove(baseFolderName());
   }

   @BeforeMethod(alwaysRun = true)
   @Override
   public void createBeforeMethod() throws Throwable {
      Util.recursiveFileRemove(baseFolderName());
      destroy();
      super.createBeforeMethod();
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder builder) {
      builder.persistence().addSoftIndexFileStore();
   }

   public void testSetGetPersistent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.set("key", "value")).isEqualTo(OK);
      assertThat(redis.get("key")).isEqualTo("value");
   }

   public void testIteration() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.keys("*")).isEmpty();

      assertThat(redis.set("test-key", "value")).isEqualTo(OK);
      assertThat(redis.keys("*")).hasSize(1).containsExactly("test-key");
      assertThat(redis.keys("test-*")).hasSize(1).containsExactly("test-key");
      assertThat(redis.keys("other-*")).isEmpty();
   }

   public void testBatchingScan() {
      RedisCommands<String, String> redis = redisConnection.sync();

      Set<String> expected = new HashSet<>();
      for (int i = 0; i < 100; i++) {
         String k = "prefix-" + i;
         expected.add(k);
         assertThat(redis.set(k, "value")).isEqualTo(OK);
      }

      Set<String> actual = new HashSet<>();
      ScanArgs args = ScanArgs.Builder
            .limit(10)
            .match("prefix-*");
      for (KeyScanCursor<String> cursor = redis.scan(args);; cursor = redis.scan(cursor, args)) {
         actual.addAll(cursor.getKeys());
         if (cursor.isFinished())
            break;
      }

      assertThat(actual)
            .hasSize(expected.size())
            .isEqualTo(expected);
   }

   protected String baseFolderName() {
      return getClass().getSimpleName();
   }

   protected String nodeId() {
      return "";
   }
}
