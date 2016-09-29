package org.infinispan.functional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.manager.PersistenceManager;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; && Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.FunctionalCachestoreTest")
public class FunctionalCachestoreTest extends AbstractFunctionalOpTest {
   // As the functional API should not have side effects, it's hard to verify its execution when it does not
   // have any return value.
   static AtomicInteger invocationCount = new AtomicInteger();

   public FunctionalCachestoreTest() {
      isPersistenceEnabled = true;
   }

   @Test(dataProvider = "owningModeAndMethod")
   public void testLoad(boolean isSourceOwner, Method method) {
      Object key = getKey(isSourceOwner);

      List<Cache<Object, Object>> owners = caches(DIST).stream()
            .filter(cache -> cache.getAdvancedCache().getDistributionManager().getLocality(key).isLocal())
            .collect(Collectors.toList());

      method.action.eval(key, wo, rw,
            (Consumer<ReadEntryView<Object, String>> & Serializable) view -> assertFalse(view.find().isPresent()),
            (Consumer<WriteEntryView<String>> & Serializable) view -> view.set("value"), () -> invocationCount);

      assertInvocations(2);

      caches(DIST).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      caches(DIST).forEach(cache -> cache.evict(key));
      caches(DIST).forEach(cache -> assertFalse(cache.getAdvancedCache().getDataContainer().containsKey(key), getAddress(cache).toString()));
      owners.forEach(cache -> {
         DummyInMemoryStore store = getStore(cache);
         assertTrue(store.contains(key), getAddress(cache).toString());
      });

      method.action.eval(key, wo, rw,
            (Consumer<ReadEntryView<Object, String>> & Serializable) view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
            },
            (Consumer<WriteEntryView<String>> & Serializable) view -> {}, () -> invocationCount);

      assertInvocations(4);
   }

   public DummyInMemoryStore getStore(Cache cache) {
      Set<DummyInMemoryStore> stores = cache.getAdvancedCache().getComponentRegistry().getComponent(PersistenceManager.class).getStores(DummyInMemoryStore.class);
      return stores.iterator().next();
   }

   @Test(dataProvider = "methods")
   public void testLoadLocal(Method method) {
      Integer key = 1;

      method.action.eval(key, lwo, lrw,
         (Consumer<ReadEntryView<Integer, String>> & Serializable) view -> assertFalse(view.find().isPresent()),
         (Consumer<WriteEntryView<String>> & Serializable) view -> view.set("value"), () -> invocationCount);

      assertInvocations(1);

      Cache<Integer, String> cache = cacheManagers.get(0).getCache();
      assertEquals(cache.get(key), "value");
      cache.evict(key);
      assertFalse(cache.getAdvancedCache().getDataContainer().containsKey(key));

      DummyInMemoryStore store = getStore(cache);
      assertTrue(store.contains(key));

      method.action.eval(key, lwo, lrw,
         (Consumer<ReadEntryView<Object, String>> & Serializable) view -> {
            assertTrue(view.find().isPresent());
            assertEquals(view.get(), "value");
         },
         (Consumer<WriteEntryView<String>> & Serializable) view -> {}, () -> invocationCount);

      assertInvocations(2);
   }

   @Override
   protected AtomicInteger invocationCount() {
      return invocationCount;
   }
}
