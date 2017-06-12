package org.infinispan.functional;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.FunctionalL1Test")
public class FunctionalL1Test extends AbstractFunctionalOpTest {
   @Override
   protected void configureCache(ConfigurationBuilder builder) {
      super.configureCache(builder);
      if (builder.clustering().cacheMode().isDistributed()) {
         builder.clustering().l1().enable();
      }
   }

   @Test(dataProvider = "owningModeAndWriteMethod")
   public void testEntryInvalidated(boolean isOwner, WriteMethod method) {
      Cache<Object, String> primary = cache(isOwner ? 0 : 2, DIST);
      Cache<Object, String> backup = cache(1, DIST);
      Cache<Object, String> reader = cache(3, DIST);
      Cache<Object, String> nonOwner = cache(isOwner ? 2 : 0, DIST);
      Object KEY = getKeyForCache(primary, backup);
      primary.put(KEY, "value");

      assertNoEntry(reader, KEY);
      assertEquals("value", reader.get(KEY));

      assertEntry(primary, KEY, "value", false);
      assertEntry(backup, KEY, "value", false);
      assertEntry(reader, KEY, "value", true);
      assertNoEntry(nonOwner, KEY);

      FunctionalMapImpl<Object, String> functionalMap = FunctionalMapImpl.create(this.<Object, String>cache(0, DIST).getAdvancedCache());
      FunctionalMap.WriteOnlyMap<Object, String> wo = WriteOnlyMapImpl.create(functionalMap);
      FunctionalMap.ReadWriteMap<Object, String> rw = ReadWriteMapImpl.create(functionalMap);
      Function<ReadEntryView<Object, String>, String> readFunc = MarshallableFunctions.returnReadOnlyFindOrNull();
      method.eval(KEY, wo, rw, readFunc, (view, nil) -> view.set("value2"), FunctionalL1Test.class);

      assertEntry(primary, KEY, "value2", false);
      assertEntry(backup, KEY, "value2", false);
      assertNoEntry(reader, KEY);
      assertNoEntry(nonOwner, KEY);
   }

   private static void assertNoEntry(Cache<Object, String> cache, Object KEY) {
      assertEquals(null, cache.getAdvancedCache().getDataContainer().get(KEY));
   }

   private static void assertEntry(Cache<Object, String> cache, Object KEY, String expectedValue, boolean isL1) {
      InternalCacheEntry<Object, String> ice = cache.getAdvancedCache().getDataContainer().get(KEY);
      assertNotNull(ice);
      assertEquals(expectedValue, ice.getValue());
      assertEquals(ice.toString(), isL1, ice.isL1Entry());
   }
}
