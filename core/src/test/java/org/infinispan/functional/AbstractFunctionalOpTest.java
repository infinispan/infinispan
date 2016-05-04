package org.infinispan.functional;

import java.io.Serializable;
import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.commons.api.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.commons.api.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.Cache;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; && Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.AbstractFunctionalOpTest")
public abstract class AbstractFunctionalOpTest extends AbstractFunctionalTest {
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

   WriteOnlyMap<Object, String> wo;
   ReadWriteMap<Object, String> rw;

   public AbstractFunctionalOpTest() {
      numNodes = 4;
      numDistOwners = 2;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   @BeforeMethod
   public void createBeforeMethod() throws Throwable {
      super.createBeforeMethod();
      this.wo = WriteOnlyMapImpl.create(fmapD1).withParams(Param.FutureMode.COMPLETED);
      this.rw = ReadWriteMapImpl.create(fmapD1).withParams(Param.FutureMode.COMPLETED);
   }

   static enum Method {
      WO_EVAL((key, wo, rw, read, write) ->
            wo.eval(key, write).join()),
      WO_EVAL_VALUE((key, wo, rw, read, write) ->
            wo.eval(key, null, (BiConsumer<String, WriteEntryView<String>> & Serializable)
                  (v, view) -> write.accept(view)).join()),
      WO_EVAL_MANY((key, wo, rw, read, write) ->
            wo.evalMany(Collections.singleton(key), write).join()),
      WO_EVAL_MANY_ENTRIES((key, wo, rw, read, write) -> 
            wo.evalMany(Collections.singletonMap(key, null), (BiConsumer<String, WriteEntryView<String>> & Serializable)
                  (v, view) -> write.accept(view)).join()),
      RW_EVAL((key, wo, rw, read, write) ->
            rw.eval(key, (Function<ReadWriteEntryView<Object, String>, Object> & Serializable)
                  view -> { read.accept(view); write.accept(view); return null; }).join()),
      RW_EVAL_VALUE((key, wo, rw, read, write) ->
            rw.eval(key, null, (BiFunction<String, ReadWriteEntryView<Object, String>, Object> & Serializable)
                  (v, view) -> { read.accept(view); write.accept(view); return null; }).join()),
      RW_EVAL_MANY((key, wo, rw, read, write) ->
            rw.evalMany(Collections.singleton(key), (Function<ReadWriteEntryView<Object, String>, Object> & Serializable)
                  view -> { read.accept(view); write.accept(view); return null; }).forEach(v -> {})),
      RW_EVAL_MANY_ENTRIES((key, wo, rw, read, write) ->
            rw.evalMany(Collections.singletonMap(key, null), (BiFunction<String, ReadWriteEntryView<Object, String>, Object> & Serializable)
                  (v, view) -> { read.accept(view); write.accept(view); return null; }).forEach(v -> {})),
      ;

      final Performer action;

      private Method(Performer action) {
         this.action = action;
      }

      @FunctionalInterface
      static interface Performer {
         void eval(Object key,
               WriteOnlyMap<Object, String> wo, ReadWriteMap<Object, String> rw,
               Consumer<ReadEntryView<Object, String>> read, Consumer<WriteEntryView<String>> write);
      }
   }
}
