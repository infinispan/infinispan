package org.infinispan.functional;

import java.io.Serializable;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.FunctionalMap.ReadOnlyMap;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; && Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.AbstractFunctionalOpTest")
public abstract class AbstractFunctionalReadOnlyOpTest extends AbstractFunctionalTest {
   @DataProvider(name = "methods")
   public static Object[][] methods() {
      return Stream.of(Method.values()).map(m -> new Object[] { m }).toArray(Object[][]::new);
   }

   @DataProvider(name = "owningModeAndMethod")
   public static Object[][] owningModeAndMethod() {
      return Stream.of(Boolean.TRUE, Boolean.FALSE)
            .flatMap(isSourceOwner -> Stream.of(Method.values())
                  .map(method -> new Object[] { isSourceOwner, method }))
            .toArray(Object[][]::new);
   }

   static Address getAddress(Cache<Object, Object> cache) {
      return cache.getAdvancedCache().getRpcManager().getAddress();
   }

   ReadOnlyMap<Object, String> ro;
   ReadOnlyMap<Integer, String> lro;

   public AbstractFunctionalReadOnlyOpTest() {
      numNodes = 4;
      numDistOwners = 2;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   @BeforeMethod
   public void createBeforeMethod() throws Throwable {
      super.createBeforeMethod();
      this.ro = ReadOnlyMapImpl.create(fmapD1).withParams(Param.FutureMode.COMPLETED);
      this.lro = ReadOnlyMapImpl.create(fmapL1).withParams(Param.FutureMode.COMPLETED);
   }

   protected Object getKey(boolean isOwner) {
      Object key;
      if (isOwner) {
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
      return key;
   }

   enum Method {
      RO_EVAL((key, ro, read) ->
            ro.eval(key,
               (Function<ReadEntryView<Object, String>, String> & Serializable) (view -> { read.accept(view); return null; })
            ).join()),
      RO_EVAL_MANY((key, ro, read) ->
            ro.evalMany(Collections.singleton(key),
               (Function<ReadEntryView<Object, String>, String> & Serializable) (view -> { read.accept(view); return null; })
            ).forEach(v -> {})),
      ;

      final Performer action;

      Method(Performer action) {
         this.action = action;
      }

      @FunctionalInterface
      interface Performer<K> {
         void eval(K key,
                   ReadOnlyMap<K, String> ro,
                   Consumer<ReadEntryView<Object, String>> read);
      }
   }
}
