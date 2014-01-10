package org.infinispan.xsite.backupfailure;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.AssertJUnit.*;

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
      try {
         cache("LON", 0).put("k", "v");
         checkFailOnBackupFailure();
      } catch (CacheException e) {
         checkNonFailOnBackupFailure();
      }
      assertEquals("v", cache("LON", 1).get("k"));
      assertTrue(failureInterceptor.putFailed);
      assertNull(backup("LON").get("k"));
   }

   public void testRemoveFailure() {
      failureInterceptor.disable();
      cache("LON", 0).put("k", "v");
      assertEquals("v", cache("LON", 1).get("k"));
      assertEquals("v", backup("LON").get("k"));

      failureInterceptor.enable();
      try {
         cache("LON", 0).remove("k");
         checkFailOnBackupFailure();
      } catch (CacheException e) {
         checkNonFailOnBackupFailure();
      }

      assertNull(cache("LON", 0).get("k"));
      assertNull(cache("LON", 1).get("k"));

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
      }

      assertEquals("v2", cache("LON", 0).get("k"));
      assertEquals("v2", cache("LON", 1).get("k"));
      //the ReplaceCommand is transformed in a PutKeyValueCommand when it succeeds in the originator site!
      assertTrue(failureInterceptor.putFailed);
      assertEquals("v", backup("LON").get("k"));
   }

   public void testClearFailure() {
      failureInterceptor.disable();
      cache("LON", 0).put("k1", "v1");
      cache("LON", 0).put("k2", "v2");
      cache("LON", 0).put("k3", "v3");

      failureInterceptor.enable();
      try {
         cache("LON", 1).clear();
         checkFailOnBackupFailure();
      } catch (CacheException e) {
         checkNonFailOnBackupFailure();
      }

      assertNull(cache("LON", 0).get("k1"));
      assertNull(cache("LON", 0).get("k2"));
      assertNull(cache("LON", 1).get("k3"));

      assertTrue(failureInterceptor.clearFailed);
      assertEquals("v1", backup("LON").get("k1"));
      assertEquals("v2", backup("LON").get("k2"));
      assertEquals("v3", backup("LON").get("k3"));
   }

   public void testPutMapFailure() {
      Map toAdd = new HashMap();
      for (int i = 0; i < 100; i++) {
         toAdd.put("k" + i, "v" + i);
      }
      try {
         cache("LON", 0).putAll(toAdd);
         checkFailOnBackupFailure();
      } catch (CacheException e) {
         checkNonFailOnBackupFailure();
      }

      for (int i = 0; i < 100; i++) {
         assertEquals("v" + i, cache("LON", i % 2).get("k" + i));
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
