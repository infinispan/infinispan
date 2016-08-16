package org.infinispan.xsite;

import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test (groups = "xsite", testName = "xsite.NoFailureAsyncReplWarnFailurePolicyTest")
public class NoFailureAsyncReplWarnFailurePolicyTest extends BaseSiteUnreachableTest {

   public NoFailureAsyncReplWarnFailurePolicyTest() {
      lonBackupStrategy = BackupConfiguration.BackupStrategy.SYNC;
      lonBackupFailurePolicy = BackupFailurePolicy.WARN;
   }

   public void testNoFailures() {
      cache("LON", 0).put("k", "v");
      assertEquals(cache("LON", 0).get("k"), "v");
      assertEquals(cache("LON", 1).get("k"), "v");

      cache("LON", 1).remove("k");
      assertNull(cache("LON", 0).get("k"));
      assertNull(cache("LON", 1).get("k"));

      cache("LON", 0).putAll(Collections.singletonMap("k", "v"));
      assertEquals(cache("LON", 0).get("k"), "v");
      assertEquals(cache("LON", 1).get("k"), "v");
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }
}
