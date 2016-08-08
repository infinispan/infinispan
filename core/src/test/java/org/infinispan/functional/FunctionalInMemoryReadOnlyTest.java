package org.infinispan.functional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.FunctionalMap;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; && Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.FunctionalInMemoryReadOnlyTest")
public class FunctionalInMemoryReadOnlyTest extends AbstractFunctionalReadOnlyOpTest {
   public FunctionalInMemoryReadOnlyTest() {
      isPersistenceEnabled = false;
   }

   @Test(dataProvider = "owningModeAndMethod")
   public void testLoad(boolean isOwner, Method method) {
      Object key = getKey(isOwner);

      method.action.eval(key, ro,
            (Consumer<ReadEntryView<Object, String>> & Serializable) view -> assertFalse(view.find().isPresent()));

      // we can't add from read-only cache, so we put manually:
      cache(0, DIST).put(key, "value");

      caches(DIST).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      caches(DIST).forEach(cache -> {
         if (cache.getAdvancedCache().getDistributionManager().getLocality(key).isLocal()) {
            assertTrue(cache.getAdvancedCache().getDataContainer().containsKey(key), getAddress(cache).toString());
         } else {
            assertFalse(cache.getAdvancedCache().getDataContainer().containsKey(key), getAddress(cache).toString());
         }
      });

      method.action.eval(key, ro,
            (Consumer<ReadEntryView<Object, String>> & Serializable) view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
            });
   }

   @Test(dataProvider = "methods")
   public void testLoadLocal(Method method) {
      Integer key = 1;

      method.action.eval(key, lro,
         (Consumer<ReadEntryView<Object, String>> & Serializable) view -> assertFalse(view.find().isPresent()));

      // we can't add from read-only cache, so we put manually:
      Cache<Integer, String> cache = cacheManagers.get(0).getCache();
      cache.put(key, "value");

      assertEquals(cache.get(key), "value");

      method.action.eval(key, lro,
         (Consumer<ReadEntryView<Object, String>> & Serializable) view -> {
            assertTrue(view.find().isPresent());
            assertEquals(view.get(), "value");
         });
   }

   @Test(dataProvider = "owningModeAndMethod")
   public void testOnMissingValue(boolean isOwner, Method method) {
      testOnMissingValue(getKey(isOwner), ro, method);
   }

   @Test(dataProvider = "methods")
   public void testOnMissingValueLocal(Method method) {
      testOnMissingValue(0, ReadOnlyMapImpl.create(fmapL1).withParams(Param.FutureMode.COMPLETED), method);
   }

   private <K> void testOnMissingValue(K key, FunctionalMap.ReadOnlyMap<K, String> ro, Method method) {
      assertEquals(ro.eval(key,
         (Function<ReadEntryView<K, String>, Boolean> & Serializable) (view -> view.find().isPresent())).join(), Boolean.FALSE);
      try {
         method.action.eval(key, ro, (Consumer<ReadEntryView<K, String>> & Serializable) view -> view.get());
         fail("Should throw CacheException:NoSuchElementException");
      } catch (CacheException e) { // catches RemoteException, too
         assertEquals(e.getCause().getClass(), NoSuchElementException.class);
      }
   }
}
