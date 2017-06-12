package org.infinispan.multimap.impl;

import static java.lang.String.format;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.multimap.impl.MultimapTestUtils.EMPTY_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.JULIEN;
import static org.infinispan.multimap.impl.MultimapTestUtils.NAMES_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.NULL_KEY;
import static org.infinispan.multimap.impl.MultimapTestUtils.NULL_USER;
import static org.infinispan.multimap.impl.MultimapTestUtils.OIHANA;
import static org.infinispan.multimap.impl.MultimapTestUtils.PEPE;
import static org.infinispan.multimap.impl.MultimapTestUtils.RAMON;
import static org.infinispan.multimap.impl.MultimapTestUtils.assertMultimapCacheSize;
import static org.infinispan.multimap.impl.MultimapTestUtils.putValuesOnMultimapCache;
import static org.infinispan.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.EncodingUtils;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.MultimapCache;
import org.infinispan.multimap.api.MultimapCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.data.Person;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistributedMultimapCacheTest")
public class DistributedMultimapCacheTest extends BaseDistFunctionalTest<String, Collection<Person>> {

   protected Map<Address, MultimapCache<String, Person>> multimapCacheCluster = new HashMap<>();

   protected boolean fromOwner;

   public DistributedMultimapCacheTest fromOwner(boolean fromOwner) {
      this.fromOwner = fromOwner;
      return this;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "fromOwner");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), fromOwner ? Boolean.TRUE : Boolean.FALSE);
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new DistributedMultimapCacheTest().fromOwner(false).cacheMode(CacheMode.DIST_SYNC).transactional(false),
            new DistributedMultimapCacheTest().fromOwner(true).cacheMode(CacheMode.DIST_SYNC).transactional(false),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();

      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         MultimapCacheManager multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cacheManager);
         multimapCacheCluster.put(cacheManager.getAddress(), multimapCacheManager.get(cacheName));
      }
   }

   @Override
   protected void initAndTest() {
      assertMultimapCacheSize(multimapCacheCluster, 0);
      putValuesOnMultimapCache(getMultimapCacheMember(), NAMES_KEY, OIHANA);
      assertValuesAndOwnership(NAMES_KEY, OIHANA);
   }

   public void testPut() {
      initAndTest();
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);

      putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN);

      assertValuesAndOwnership(NAMES_KEY, JULIEN);

      putValuesOnMultimapCache(multimapCache, EMPTY_KEY, RAMON);

      assertValuesAndOwnership(EMPTY_KEY, RAMON);

      putValuesOnMultimapCache(multimapCache, NAMES_KEY, PEPE);

      assertValuesAndOwnership(NAMES_KEY, PEPE);
   }

   public void testPutDuplicates() {
      initAndTest();
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);

      putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN);
      putValuesOnMultimapCache(multimapCache, NAMES_KEY, RAMON);

      assertValuesAndOwnership(NAMES_KEY, JULIEN);
      assertValuesAndOwnership(NAMES_KEY, RAMON);

      assertEquals(3, await(multimapCache.size()).intValue());

      putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN);

      assertValuesAndOwnership(NAMES_KEY, JULIEN);
      assertValuesAndOwnership(NAMES_KEY, RAMON);

      assertEquals(3, await(multimapCache.size()).intValue());

      putValuesOnMultimapCache(multimapCache, NAMES_KEY, JULIEN);

      assertValuesAndOwnership(NAMES_KEY, JULIEN);
      assertValuesAndOwnership(NAMES_KEY, RAMON);

      assertEquals(3, await(multimapCache.size()).intValue());
   }


   public void testRemoveKey() {
      initAndTest();
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);

      await(
            multimapCache.remove(NAMES_KEY, OIHANA).thenCompose(r1 -> {
               assertTrue(r1);
               return multimapCache.get(NAMES_KEY).thenAccept(v -> assertTrue(v.isEmpty()));
            })
      );

      assertRemovedOnAllCaches(NAMES_KEY);

      putValuesOnMultimapCache(multimapCache, EMPTY_KEY, RAMON);

      await(multimapCache.remove(EMPTY_KEY).thenCompose(r1 -> {
         assertTrue(r1);
         return multimapCache.get(EMPTY_KEY).thenAccept(v -> assertTrue(v.isEmpty()));
      }));
   }

   public void testRemoveKeyValue() {
      initAndTest();
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);

      await(multimapCache.remove("unexistingKey", OIHANA).thenAccept(r -> assertFalse(r)));
      assertValuesAndOwnership(NAMES_KEY, OIHANA);

      await(
            multimapCache.remove(NAMES_KEY, OIHANA).thenCompose(r1 -> {
                     assertTrue(r1);
                     return multimapCache.get(NAMES_KEY).thenAccept(v -> assertTrue(v.isEmpty()));
                  }
            )
      );

      assertRemovedOnAllCaches(NAMES_KEY);

      putValuesOnMultimapCache(multimapCache, EMPTY_KEY, RAMON);

      await(multimapCache.remove(EMPTY_KEY, RAMON).thenCompose(r1 -> {
         assertTrue(r1);
         return multimapCache.get(EMPTY_KEY).thenAccept(v -> assertTrue(v.isEmpty()));
      }));


      putValuesOnMultimapCache(multimapCache, NAMES_KEY, PEPE);

      await(multimapCache.remove(NAMES_KEY, PEPE).thenCompose(r1 -> {
         assertTrue(r1);
         return multimapCache.get(NAMES_KEY).thenAccept(v -> assertTrue(v.isEmpty()));
      }));
   }

   public void testRemoveWithPredicate() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember();

      await(
            multimapCache.put(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> multimapCache.put(NAMES_KEY, JULIEN))
                  .thenCompose(r2 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v -> assertEquals(2, v.size()))
      );

      assertValuesAndOwnership(NAMES_KEY, OIHANA);
      assertValuesAndOwnership(NAMES_KEY, JULIEN);

      MultimapCache<String, Person> multimapCache2 = getMultimapCacheMember(NAMES_KEY);

      await(
            multimapCache2.remove(o -> o.getName().contains("Ka"))
                  .thenCompose(r1 -> multimapCache2.get(NAMES_KEY))
                  .thenAccept(v ->
                        assertEquals(2, v.size())

                  )
      );
      assertValuesAndOwnership(NAMES_KEY, OIHANA);
      assertValuesAndOwnership(NAMES_KEY, JULIEN);

      await(
            multimapCache.remove(o -> o.getName().contains("Ju"))
                  .thenCompose(r1 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v ->
                        assertEquals(1, v.size())
                  )
      );
      assertValuesAndOwnership(NAMES_KEY, OIHANA);
      assertKeyValueNotFoundInAllCaches(NAMES_KEY, JULIEN);


      await(
            multimapCache.remove(o -> o.getName().contains("Oi"))
                  .thenCompose(r1 -> multimapCache.get(NAMES_KEY))
                  .thenAccept(v ->
                        assertTrue(v.isEmpty())
                  )
      );

      assertRemovedOnAllCaches(NAMES_KEY);
   }

   public void testGet() {
      initAndTest();
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember();

      await(multimapCache.get(NAMES_KEY).thenAccept(v -> {
         assertTrue(v.contains(OIHANA));
      }));

      await(multimapCache.getEntry(NAMES_KEY).thenAccept(v -> {
         assertTrue(v.isPresent() && v.get().getKey().equals(NAMES_KEY) && v.get().getValue().contains(OIHANA));
      }));
      await(multimapCache.put(EMPTY_KEY, RAMON).thenCompose(r3 -> multimapCache.get(EMPTY_KEY)).thenAccept(v -> {
         assertTrue(v.contains(RAMON));
      }));
   }

   public void testGetEmpty() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember();

      await(
            multimapCache.get(NAMES_KEY)
                  .thenAccept(v -> {
                           assertTrue(v.isEmpty());
                        }
                  )

      );

      await(multimapCache.getEntry(NAMES_KEY).thenAccept(v -> {
         assertTrue(!v.isPresent());
      }));
   }

   public void testGetAndModifyResults() {
      initAndTest();
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);

      Person pepe = new Person("Pepe");

      await(
            multimapCache.get(NAMES_KEY)
                  .thenAccept(v -> {
                           List<Person> modifiedList = new ArrayList<>(v);
                           modifiedList.add(pepe);
                        }
                  )

      );

      assertKeyValueNotFoundInAllCaches(NAMES_KEY, pepe);
   }

   public void testContainsKey() {
      initAndTest();

      multimapCacheCluster.values().forEach(mc -> {
         await(
               mc.containsKey("other")
                     .thenAccept(containsKey -> assertFalse(containsKey))
         );
         await(
               mc.containsKey(NAMES_KEY)
                     .thenAccept(containsKey -> assertTrue(containsKey))
         );
         await(
               mc.containsKey(EMPTY_KEY)
                     .thenAccept(containsKey -> assertFalse(containsKey))
         );
      });
   }

   public void testContainsValue() {
      initAndTest();
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);
      putValuesOnMultimapCache(multimapCache, NAMES_KEY, PEPE);

      multimapCacheCluster.values().forEach(mc -> {
         await(
               mc.containsValue(RAMON)
                     .thenAccept(containsValue -> assertFalse(containsValue))
         );
         await(
               mc.containsValue(OIHANA)
                     .thenAccept(containsValue -> assertTrue(containsValue))
         );
         await(
               mc.containsValue(PEPE)
                     .thenAccept(containsValue -> assertTrue(containsValue))
         );
      });
   }

   public void testContainEntry() {
      initAndTest();

      putValuesOnMultimapCache(multimapCacheCluster, EMPTY_KEY, PEPE);

      multimapCacheCluster.values().forEach(mc -> {
         await(
               mc.containsEntry(NAMES_KEY, RAMON)
                     .thenAccept(containsValue -> assertFalse(containsValue))
         );
         await(
               mc.containsEntry(NAMES_KEY, OIHANA)
                     .thenAccept(containsValue -> assertTrue(containsValue))
         );
         await(
               mc.containsEntry(EMPTY_KEY, RAMON)
                     .thenAccept(containsValue -> assertFalse(containsValue))
         );
         await(
               mc.containsEntry(EMPTY_KEY, PEPE)
                     .thenAccept(containsValue -> assertTrue(containsValue))
         );
      });
   }

   public void testSize() {
      String anotherKey = "firstNames";
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);

      await(
            multimapCache.put(NAMES_KEY, OIHANA)
                  .thenCompose(r1 -> multimapCache.put(NAMES_KEY, JULIEN))
                  .thenCompose(r2 -> multimapCache.put(anotherKey, OIHANA))
                  .thenCompose(r3 -> multimapCache.put(anotherKey, JULIEN))
                  .thenCompose(r4 -> multimapCache.size())
                  .thenAccept(s -> {
                     assertEquals(4, s.intValue());
                     assertValuesAndOwnership(NAMES_KEY, JULIEN);
                     assertValuesAndOwnership(NAMES_KEY, OIHANA);
                     assertValuesAndOwnership(anotherKey, JULIEN);
                     assertValuesAndOwnership(anotherKey, OIHANA);
                  })
                  .thenCompose(r1 -> multimapCache.remove(NAMES_KEY, JULIEN))
                  .thenCompose(r2 -> multimapCache.remove(NAMES_KEY, OIHANA))
                  .thenCompose(r2 -> multimapCache.remove(NAMES_KEY, JULIEN))
                  .thenCompose(r3 -> multimapCache.put(anotherKey, JULIEN))
                  .thenCompose(r4 -> multimapCache.size())
                  .thenAccept(s -> {
                     assertEquals(2, s.intValue());
                     assertValuesAndOwnership(anotherKey, JULIEN);
                     assertValuesAndOwnership(anotherKey, OIHANA);
                  })
      );

   }

   public void testGetEntry() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);

      await(
            multimapCache.getEntry(NAMES_KEY)
                  .thenAccept(maybeEntry -> {
                           assertFalse(NAMES_KEY, maybeEntry.isPresent());
                        }
                  )
      );

      await(
            multimapCache.put(NAMES_KEY, JULIEN)
                  .thenCompose(r3 -> multimapCache.getEntry(NAMES_KEY))
                  .thenAccept(maybeEntry -> {
                           assertTrue(NAMES_KEY, maybeEntry.isPresent());
                        }
                  )
      );

      await(multimapCache.put(EMPTY_KEY, RAMON).thenCompose(r3 -> multimapCache.getEntry(EMPTY_KEY)).thenAccept(v -> {
         assertTrue(v.isPresent() && v.get().getKey().equals(EMPTY_KEY) && v.get().getValue().contains(RAMON));
      }));
   }

   public void testPutWithNull() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.put(NULL_KEY, OIHANA));
      expectException(NullPointerException.class, "value can't be null", () -> multimapCache.put(NAMES_KEY, NULL_USER));
   }

   public void testGetWithNull() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.get(NULL_KEY));
   }

   public void testGetEntryWithNull() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.getEntry(NULL_KEY));
   }

   public void testRemoveKeyValueWithNull() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.remove(NULL_KEY, RAMON));
      expectException(NullPointerException.class, "value can't be null", () -> multimapCache.remove(NAMES_KEY, NULL_USER));
   }

   public void testRemoveKeyWithNulll() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.remove((String) NULL_KEY));
   }

   public void testRemoveWithNullPredicate() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);
      expectException(NullPointerException.class, "predicate can't be null", () -> multimapCache.remove((Predicate<? super Person>) null));
   }

   public void testContainsKeyWithNull() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.containsKey(NULL_KEY));
   }

   public void testContainsValueWithNull() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);
      expectException(NullPointerException.class, "value can't be null", () -> multimapCache.containsValue(NULL_USER));
   }

   public void testContainsEntryWithNull() {
      MultimapCache<String, Person> multimapCache = getMultimapCacheMember(NAMES_KEY);
      expectException(NullPointerException.class, "key can't be null", () -> multimapCache.containsEntry(NULL_KEY, OIHANA));
      expectException(NullPointerException.class, "value can't be null", () -> multimapCache.containsEntry(NAMES_KEY, NULL_USER));
   }

   protected MultimapCache getMultimapCacheMember() {
      return multimapCacheCluster.values().stream().findFirst().orElseThrow(() -> new IllegalStateException("Cluster is empty"));
   }

   protected MultimapCache getMultimapCacheMember(String key) {
      Cache<String, Collection<Person>> cache = fromOwner ? getFirstOwner(key) : getFirstNonOwner(key);
      return multimapCacheCluster.get(cache.getCacheManager().getAddress());
   }

   protected MultimapCache getMultimapCacheFirstOwner(String key) {
      Cache<String, Collection<Person>> cache = getFirstOwner(key);
      return multimapCacheCluster.get(cache.getCacheManager().getAddress());
   }

   protected void assertValuesAndOwnership(String key, Person value) {
      assertOwnershipAndNonOwnership(key, l1CacheEnabled);
      assertOnAllCaches(key, value);
   }

   protected void assertKeyValueNotFoundInAllCaches(String key, Person value) {
      for (Map.Entry<Address, MultimapCache<String, Person>> entry : multimapCacheCluster.entrySet()) {
         await(entry.getValue().get(key).thenAccept(v -> {
                  assertNotNull(format("values on the key %s must be not null", key), v);
                  assertFalse(format("values on the key '%s' must not contain '%s' on node '%s'", key, value, entry.getKey()), v.contains(value));
               })

         );
      }
   }

   protected void assertKeyValueFoundInOwners(String key, Person value) {
      Cache<String, Collection<Person>> firstOwner = getFirstOwner(key);
      Cache<String, Collection<Person>> secondNonOwner = getSecondNonOwner(key);

      MultimapCache<String, Person> mcFirstOwner = multimapCacheCluster.get(firstOwner.getCacheManager().getAddress());
      MultimapCache<String, Person> mcSecondOwner = multimapCacheCluster.get(secondNonOwner.getCacheManager().getAddress());


      await(mcFirstOwner.get(key).thenAccept(v -> {
               assertTrue(format("firstOwner '%s' must contain key '%s' value '%s' pair", firstOwner.getCacheManager().getAddress(), key, value), v.contains(value));
            })
      );

      await(mcSecondOwner.get(key).thenAccept(v -> {
               assertTrue(format("secondOwner '%s' must contain key '%s' value '%s' pair", secondNonOwner.getCacheManager().getAddress(), key, value), v.contains(value));
            })
      );
   }

   @Override
   protected void assertOwnershipAndNonOwnership(Object key, boolean allowL1) {
      for (Cache cache : caches) {
         Wrapper keyWrapper = cache.getAdvancedCache().getKeyWrapper();
         Encoder keyEncoder = cache.getAdvancedCache().getKeyEncoder();
         Object keyToBeChecked = keyEncoder != null && keyWrapper != null ? EncodingUtils.toStorage(key, keyEncoder, keyWrapper) : key;
         DataContainer dc = cache.getAdvancedCache().getDataContainer();
         InternalCacheEntry ice = dc.get(keyToBeChecked);
         if (isOwner(cache, keyToBeChecked)) {
            assertNotNull(ice);
            assertTrue(ice instanceof ImmortalCacheEntry);
         } else {
            if (allowL1) {
               assertTrue("ice is null or L1Entry", ice == null || ice.isL1Entry());
            } else {
               // Segments no longer owned are invalidated asynchronously
               eventuallyEquals("Fail on non-owner cache " + addressOf(cache) + ": dc.get(" + key + ")",
                     null, () -> dc.get(keyToBeChecked));
            }
         }
      }
   }

   protected void assertOnAllCaches(Object key, Person value) {
      for (Map.Entry<Address, MultimapCache<String, Person>> entry : multimapCacheCluster.entrySet()) {
         await(entry.getValue().get((String) key).thenAccept(v -> {
                  assertNotNull(format("values on the key %s must be not null", key), v);
                  assertTrue(format("values on the key '%s' must contain '%s' on node '%s'", key, value, entry.getKey()),
                        v.contains(value));
               })

         );
      }
   }
}
