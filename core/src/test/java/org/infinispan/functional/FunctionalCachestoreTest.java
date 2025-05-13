package org.infinispan.functional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; and Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.FunctionalCachestoreTest")
public class FunctionalCachestoreTest extends AbstractFunctionalOpTest {
   @Override
   public Object[] factory() {
      return new Object[]{
            new FunctionalCachestoreTest().persistence(true).passivation(false),
            new FunctionalCachestoreTest().persistence(true).passivation(true)
      };
   }

   @Override
   protected String parameters() {
      return "[passivation=" + passivation + "]";
   }

   @Test(dataProvider = "owningModeAndWriteMethod")
   public void testWriteLoad(boolean isSourceOwner, WriteMethod method) throws Exception {
      Object key = getKey(isSourceOwner, DIST);

      List<Cache<Object, Object>> owners = caches(DIST).stream()
            .filter(cache -> cache.getAdvancedCache().getDistributionManager().getCacheTopology().isReadOwner(key))
            .collect(Collectors.toList());

      method.eval(key, wo, rw,
            view -> {
               assertFalse(view.find().isPresent());
               return null;
            },
            (view, nil) -> view.set("value"), getClass());

      assertInvocations(2);

      caches(DIST).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      // Staggered gets could arrive after evict command and that would reload the entry into DC
      advanceGenerationsAndAwait(10, TimeUnit.SECONDS);
      caches(DIST).forEach(cache -> cache.evict(key));
      caches(DIST).forEach(cache -> assertFalse(cache.getAdvancedCache().getDataContainer().containsKey(key), getAddress(cache).toString()));
      owners.forEach(cache -> {
         DummyInMemoryStore store = getStore(cache);
         assertTrue(store.contains(key), getAddress(cache).toString());
      });

      resetInvocationCount();

      method.eval(key, wo, rw,
            view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
               return null;
            },
            (view, nil) -> {
            }, getClass());

      if (method.isMany) {
         assertInvocations(2);
      } else {
         assertInvocations(1);
      }
   }

   public DummyInMemoryStore getStore(Cache cache) {
      Set<DummyInMemoryStore> stores = TestingUtil.extractComponent(cache, PersistenceManager.class).getStores(DummyInMemoryStore.class);
      return stores.iterator().next();
   }

   @Test(dataProvider = "writeMethods")
   public void testWriteLoadLocal(WriteMethod method) {
      Integer key = 1;

      method.eval(key, lwo, lrw,
            view -> {
               assertFalse(view.find().isPresent());
               return null;
            },
            (view, nil) -> view.set("value"), getClass());

      assertInvocations(1);

      Cache<Integer, String> cache = cacheManagers.get(0).getCache();
      assertEquals(cache.get(key), "value");
      cache.evict(key);
      assertFalse(cache.getAdvancedCache().getDataContainer().containsKey(key));

      DummyInMemoryStore store = getStore(cache);
      assertTrue(store.contains(key));

      method.eval(key, lwo, lrw,
            view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
               return null;
            },
            (view, nil) -> {
            }, getClass());

      assertInvocations(2);
   }

   @Test(dataProvider = "owningModeAndReadMethod")
   public void testReadLoad(boolean isSourceOwner, ReadMethod method) {
      Object key = getKey(isSourceOwner, DIST);
      List<Cache<Object, Object>> owners = caches(DIST).stream()
            .filter(cache -> cache.getAdvancedCache().getDistributionManager().getCacheTopology().isReadOwner(key))
            .collect(Collectors.toList());

      assertTrue(method.eval(key, ro, view -> {
         assertFalse(view.find().isPresent());
         return true;
      }));

      // we can't add from read-only cache, so we put manually:
      cache(0, DIST).put(key, "value");

      caches(DIST).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      caches(DIST).forEach(cache -> cache.evict(key));
      caches(DIST).forEach(cache -> assertFalse(cache.getAdvancedCache().getDataContainer().containsKey(key), getAddress(cache).toString()));
      owners.forEach(cache -> {
         Set<DummyInMemoryStore> stores = ComponentRegistry.componentOf(cache, PersistenceManager.class).getStores(DummyInMemoryStore.class);
         DummyInMemoryStore store = stores.iterator().next();
         assertTrue(store.contains(key), getAddress(cache).toString());
      });

      assertEquals(method.eval(key, ro,
            view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
               return "OK";
            }), "OK");
   }
}
