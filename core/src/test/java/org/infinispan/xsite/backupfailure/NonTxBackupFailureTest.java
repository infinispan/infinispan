package org.infinispan.xsite.backupfailure;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "xsite", testName = "xsite.backupfailure.NonTxBackupFailureTest")
public class NonTxBackupFailureTest extends BaseBackupFailureTest {

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   public void testPutFailure() {
      failureInterceptor.enable();
      try {
         cache("LON", 0).put("k", "v");
         checkFailOnBackupFailure();
      } catch (CacheException e) {
         checkNonFailOnBackupFailure();
      } finally {
         failureInterceptor.disable();
      }

      //in triangle, if an exception is received, the originator doesn't wait for the ack from backup
      //it is possible to check the value before the backup handles the BackupWriteCommand.
      eventuallyEquals("v", () -> cache("LON", 1).get("k"));
      assertTrue(failureInterceptor.putFailed);
      assertNull(backup("LON").get("k"));
   }

   public void testRemoveFailure() {
      cache("LON", 0).put("k", "v");
      assertEquals("v", cache("LON", 1).get("k"));
      assertEquals("v", backup("LON").get("k"));

      failureInterceptor.enable();
      try {
         cache("LON", 0).remove("k");
         checkFailOnBackupFailure();
      } catch (CacheException e) {
         checkNonFailOnBackupFailure();
      } finally {
         failureInterceptor.disable();
      }

      eventuallyEquals(null, () -> cache("LON", 0).get("k"));
      eventuallyEquals(null, () -> cache("LON", 1).get("k"));

      assertTrue(failureInterceptor.removeFailed);
      assertEquals("v", backup("LON").get("k"));
   }

   public void testReplaceFailure() {
      failureInterceptor.disable();
      cache("LON", 0).put("k", "v");
      assertEquals("v", cache("LON", 1).get("k"));
      assertEquals("v", backup("LON").get("k"));

      failureInterceptor.enable();
      try {
         cache("LON", 0).replace("k", "v2");
         checkFailOnBackupFailure();
      } catch (CacheException e) {
         checkNonFailOnBackupFailure();
      } finally {
         failureInterceptor.disable();
      }

      eventuallyEquals("v2", () -> cache("LON", 0).get("k"));
      eventuallyEquals("v2", () -> cache("LON", 1).get("k"));
      //the ReplaceCommand is transformed in a PutKeyValueCommand when it succeeds in the originator site!
      assertTrue(failureInterceptor.putFailed);
      assertEquals("v", backup("LON").get("k"));
   }

   public void testClearFailure() {
      cache("LON", 0).put("k1", "v1");
      cache("LON", 0).put("k2", "v2");
      cache("LON", 0).put("k3", "v3");

      failureInterceptor.enable();
      try {
         cache("LON", 1).clear();
         checkFailOnBackupFailure();
      } catch (CacheException e) {
         checkNonFailOnBackupFailure();
      } finally {
         failureInterceptor.disable();
      }

      eventuallyEquals(null, () -> cache("LON", 0).get("k1"));
      eventuallyEquals(null, () -> cache("LON", 0).get("k2"));
      eventuallyEquals(null, () -> cache("LON", 0).get("k3"));

      eventuallyEquals(null, () -> cache("LON", 1).get("k1"));
      eventuallyEquals(null, () -> cache("LON", 1).get("k2"));
      eventuallyEquals(null, () -> cache("LON", 1).get("k3"));

      assertTrue(failureInterceptor.clearFailed);
      assertEquals("v1", backup("LON").get("k1"));
      assertEquals("v2", backup("LON").get("k2"));
      assertEquals("v3", backup("LON").get("k3"));
   }

   public void testPutMapFailure() {
      Map<String, String> toAdd = new HashMap<>();
      for (int i = 0; i < 100; i++) {
         toAdd.put("k" + i, "v" + i);
      }
      failureInterceptor.enable();
      try {
         cache("LON", 0).putAll(toAdd);
         checkFailOnBackupFailure();
      } catch (CacheException e) {
         checkNonFailOnBackupFailure();
      } finally {
         failureInterceptor.disable();
      }

      assertTrue(failureInterceptor.writeOnlyManyEntriesFailed);
      for (int i = 0; i < 100; i++) {
         final int keyIndex = i;
         // In the past, data was xsite-backed up from origin only after all entries were committed locally.
         // Now that the xsite-backup runs always on primary owner, the failure on remote node propagates
         // back to the origin and this causes the triangle's collector future to fail with exception.
         // Entries that are not owned locally are not committed then (as the failure comes in distribution interceptor)
         // which is below entry-wrapping, though those entries that could not be xsite-backed up are committed.
         if (lonBackupFailurePolicy != BackupFailurePolicy.FAIL) {
            eventuallyEquals("v" + keyIndex, () -> cache("LON", keyIndex % 2).get("k" + keyIndex));
         }
         assertNull(backup("LON").get("k" + i));
      }
   }


   private void checkNonFailOnBackupFailure() {
      if (!failOnBackupFailure("LON", 0)) throw new AssertionError("Should fail silently!");
   }

   private void checkFailOnBackupFailure() {
      if (failOnBackupFailure("LON", 0)) throw new AssertionError("Exception expected!");
   }
}
