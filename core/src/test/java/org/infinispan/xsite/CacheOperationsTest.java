package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "xsite", testName = "xsite.CacheOperationsTest")
public class CacheOperationsTest extends AbstractTwoSitesTest {

   @Factory
   public Object[] factory() {
      return new Object[] {
            new CacheOperationsTest().cacheMode(CacheMode.DIST_SYNC).transactional(false),
            new CacheOperationsTest().cacheMode(CacheMode.REPL_SYNC).transactional(false),
            new CacheOperationsTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC).use2Pc(false),
            new CacheOperationsTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC).use2Pc(true),
            new CacheOperationsTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC),
            new CacheOperationsTest().cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC).use2Pc(false),
            new CacheOperationsTest().cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC).use2Pc(true),
            new CacheOperationsTest().cacheMode(CacheMode.REPL_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC),
      };
   }

   public CacheOperationsTest() {
      // We need both owner an non-owner setup
      initialClusterSize = 3;
   }

   protected ConfigurationBuilder getNycActiveConfig() {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(cacheMode, transactional);
      if (lockingMode != null) {
         cb.transaction().lockingMode(lockingMode);
      }
      return cb;
   }

   protected ConfigurationBuilder getLonActiveConfig() {
      return getNycActiveConfig();
   }

   public void testRemove() {
      testRemove("LON");
      testRemove("NYC");
   }

   public void testPutAndClear() {
      testPutAndClear("LON");
      testPutAndClear("NYC");
   }

   public void testReplace() {
      testReplace("LON");
      testReplace("NYC");
   }

   public void testPutAll() {
      testPutAll("LON");
      testPutAll("NYC");
   }

   private void testRemove(String site) {
      String key = key(site);
      String val = val(site);

      cache(site, 0).put(key, val);
      assertEquals(backup(site).get(key), val);

      cache(site, 0).remove(key);
      assertNull(backup(site).get(key));

      cache(site, 0).put(key, val);
      assertEquals(backup(site).get(key), val);

      cache(site, 0).remove(key, val);
      assertNull(backup(site).get(key));
   }

   private void testReplace(String site) {
      String key = key(site);
      String val = val(site);
      cache(site, 0).put(key, val);
      Cache<Object, Object> backup = backup(site);
      assertEquals(backup.get(key), val);

      String val2 = val + 1;

      cache(site, 0).replace(key, val2);
      assertEquals(backup.get(key), val2);

      String val3 = val+2;
      cache(site, 0).replace(key, "v_non", val3);
      assertEquals(backup.get(key), val2);

      cache(site, 0).replace(key, val2, val3);
      assertEquals(backup.get(key), val3);
   }

   private void testPutAndClear(String site) {
      String key = key(site);
      String val = val(site);

      cache(site, 0).put(key, val);
      assertEquals(backup(site).get(key), val);

      cache(site, 0).clear();
      assertNull(backup(site).get(key+1));
      assertNull(backup(site).get(key));
   }

   private void testPutAll( String site) {
      Map all = new HashMap();
      String key = key(site);
      String val = val(site);

      for (int i = 0; i < 10; i++) {
         all.put(key + i, val + i);
      }
      cache(site, 0).putAll(all);
      for (int i = 0; i < 10; i++) {
         assertEquals(backup(site).get(key + i), val + i);
      }
   }

   public void testDataGetsReplicated() {
      cache("LON", 0).put("k_lon", "v_lon");
      assertNull(cache("NYC", 0).get("k_lon"));
      assertEquals(cache("LON", 1).get("k_lon"), "v_lon");
      assertEquals(cache("NYC", "lonBackup", 0).get("k_lon"), "v_lon");
      assertEquals(cache("NYC", "lonBackup", 1).get("k_lon"), "v_lon");

      cache("NYC",1).put("k_nyc", "v_nyc");
      assertEquals(cache("LON", 1).get("k_lon"), "v_lon");
      assertEquals(cache("LON", "nycBackup", 0).get("k_nyc"), "v_nyc");
      assertEquals(cache("LON", "nycBackup", 1).get("k_nyc"), "v_nyc");
      assertNull(cache("LON", 0).get("k_nyc"));

      cache("LON", 1).remove("k_lon");
      assertNull(cache("LON", 1).get("k_lon"));
      assertNull(cache("NYC", "lonBackup", 0).get("k_lon"));
      assertNull(cache("NYC", "lonBackup", 1).get("k_lon"));
   }

   public void testPutWithLocality() {
      MagicKey remoteOwnedKey = new MagicKey(cache("LON", 1));
      cache("LON", 0).put(remoteOwnedKey, "v_LON");
      assertEquals(cache("NYC", "lonBackup", 0).get(remoteOwnedKey), "v_LON");
      assertEquals(cache("NYC", "lonBackup", 1).get(remoteOwnedKey), "v_LON");

      MagicKey localOwnedKey = new MagicKey(cache("LON", 0));
      cache("LON", 0).put(localOwnedKey, "v_LON");
      assertEquals(cache("NYC", "lonBackup", 0).get(remoteOwnedKey), "v_LON");
      assertEquals(cache("NYC", "lonBackup", 1).get(remoteOwnedKey), "v_LON");
   }

   public void testFunctional() throws Exception {
      testFunctional("LON");
      testFunctional("NYC");
   }

   private void testFunctional(String site) throws Exception {
      FunctionalMapImpl<Object, Object> fmap = FunctionalMapImpl.create(cache(site, 0).getAdvancedCache());
      WriteOnlyMap<Object, Object> wo = WriteOnlyMapImpl.create(fmap);
      ReadWriteMap<Object, Object> rw = ReadWriteMapImpl.create(fmap);
      Cache<Object, Object> backup = backup(site);

      Object[] keys = {
            new MagicKey("k0", cache(site, 0), cache(site, 1)),
            new MagicKey("k1", cache(site, 1), cache(site, 0)),
            new MagicKey("k2", cache(site, 1), cache(site, 2))
      };
      for (Object key : keys) {
         wo.eval(key, "v0", MarshallableFunctions.setValueConsumer()).join();
         assertEquals("v0", backup.get(key));
      }

      for (Object key : keys) {
         wo.eval(key, MarshallableFunctions.removeConsumer()).join();
         assertEquals(null, backup.get(key));
      }

      wo.evalMany(map(keys, "v1"), MarshallableFunctions.setValueConsumer()).join();
      for (Object key : keys) {
         assertEquals("v1", backup.get(key));
      }

      for (Object key : keys) {
         rw.eval(key, view -> view.set(view.get() + "+2")).join();
         assertEquals("v1+2", backup.get(key));
      }

      rw.evalMany(Util.asSet(keys), view -> view.set(view.get() + "+3"))
            .forEach(ret -> assertEquals(null, ret));
      for (Object key : keys) {
         assertEquals("v1+2+3", backup.get(key));
      }

      wo.evalMany(Util.asSet(keys), MarshallableFunctions.removeConsumer()).join();
      for (Object key : keys) {
         assertEquals(null, backup.get(key));
      }

      rw.evalMany(Util.asSet(keys), view -> view.find().orElse("none"))
            .forEach(ret -> assertEquals("none", ret));
      for (Object key : keys) {
         assertEquals(null, backup.get(key));
      }

      if (transactional) {
         TransactionManager tm = cache(site, 0).getAdvancedCache().getTransactionManager();
         tm.begin();
         rw.eval(keys[0], "v4", MarshallableFunctions.setValueReturnPrevOrNull()).join();
         //read-only evalMany
         rw.evalMany(Util.asSet(keys[1], keys[2]), view -> view.find().orElse("none"))
               .forEach(ret -> assertEquals("none", ret));
         tm.commit();
         assertEquals("v4", backup.get(keys[0]));
         assertEquals(null, backup.get(keys[1]));
         assertEquals(null, backup.get(keys[2]));
      }
   }

   private static Map<Object, Object> map(Object[] keys, String value) {
      return Stream.of(keys).collect(Collectors.toMap(Function.identity(), ignored -> value));
   }
}
