package org.infinispan.functional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; && Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.FunctionalInMemoryTest")
public class FunctionalInMemoryTest extends AbstractFunctionalOpTest {
   public FunctionalInMemoryTest() {
      isPersistenceEnabled = false;
   }

   @Test(dataProvider = "owningModeAndMethod")
   public void testLoad(boolean isSourceOwner, Method method) {
      Object key;
      if (isSourceOwner) {
         // this is simple: find a key that is local to the originating node
         key = getKeyForCache(0, DIST);
      } else {
         // this is more complicated: we need a key that is *not* local to the originating node
         key = IntStream.iterate(0, i -> i + 1)
               .mapToObj(i -> "key" + i)
               .filter(k -> !cache(0, DIST).getAdvancedCache().getDistributionManager().getLocality(k).isLocal())
               .findAny()
               .get();
      }

      method.action.eval(key, wo, rw,
            (Consumer<ReadEntryView<Object, String>> & Serializable) view -> assertFalse(view.find().isPresent()),
            (Consumer<WriteEntryView<String>> & Serializable) view -> view.set("value"));

      caches(DIST).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      caches(DIST).forEach(cache -> {
         if (cache.getAdvancedCache().getDistributionManager().getLocality(key).isLocal()) {
            assertTrue(cache.getAdvancedCache().getDataContainer().containsKey(key), getAddress(cache).toString());
         } else {
            assertFalse(cache.getAdvancedCache().getDataContainer().containsKey(key), getAddress(cache).toString());
         }
      });

      method.action.eval(key, wo, rw,
            (Consumer<ReadEntryView<Object, String>> & Serializable) view -> {
               assertTrue(view.find().isPresent());
               assertEquals(view.get(), "value");
            },
            (Consumer<WriteEntryView<String>> & Serializable) view -> {});
   }
}