package org.infinispan.functional;

import java.util.Collections;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.FunctionalMap.ReadOnlyMap;
import org.infinispan.Cache;
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
   @DataProvider(name = "owningModeAndMethod")
   public static Object[][] booleans() {
      return Stream.of(Boolean.TRUE, Boolean.FALSE)
            .flatMap(isSourceOwner -> Stream.of(Method.values())
                  .map(method -> new Object[] { isSourceOwner, method }))
            .toArray(Object[][]::new);
   }

   static Address getAddress(Cache<Object, Object> cache) {
      return cache.getAdvancedCache().getRpcManager().getAddress();
   }

   ReadOnlyMap<Object, String> ro;

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
   }

   static enum Method {
      RO_EVAL((key, ro, read) ->
            ro.eval(key, view -> { read.accept(view); return null; }).join()),
      RO_EVAL_MANY((key, ro, read) ->
            ro.evalMany(Collections.singleton(key), view -> { read.accept(view); return null; }).forEach(v -> {})),
      ;

      final Performer action;

      private Method(Performer action) {
         this.action = action;
      }

      @FunctionalInterface
      static interface Performer {
         void eval(Object key,
               ReadOnlyMap<Object, String> ro,
               Consumer<ReadEntryView<Object, String>> read);
      }
   }
}
