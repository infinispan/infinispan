package org.infinispan.functional;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.commons.api.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.commons.api.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.CountingCARD;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; && Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.AbstractFunctionalOpTest")
public abstract class AbstractFunctionalOpTest extends AbstractFunctionalTest {

   WriteOnlyMap<Object, String> wo;
   ReadWriteMap<Object, String> rw;
   WriteOnlyMap<Integer, String> lwo;
   ReadWriteMap<Integer, String> lrw;
   List<CountingCARD> countingCARDs;

   public AbstractFunctionalOpTest() {
      numNodes = 4;
      numDistOwners = 2;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @DataProvider(name = "methods")
   public static Object[][] methods() {
      return Stream.of(Method.values()).map(m -> new Object[]{m}).toArray(Object[][]::new);
   }

   @DataProvider(name = "owningModeAndMethod")
   public static Object[][] owningModeAndMethod() {
      return Stream.of(Boolean.TRUE, Boolean.FALSE)
            .flatMap(isSourceOwner -> Stream.of(Method.values())
                  .map(method -> new Object[]{isSourceOwner, method}))
            .toArray(Object[][]::new);
   }

   @DataProvider(name = "owningModeAndReadWrites")
   public static Object[][] owningModeAndReadWrites() {
      return Stream.of(Boolean.TRUE, Boolean.FALSE)
            .flatMap(isSourceOwner -> Stream.of(Method.values()).filter(m -> m.doesRead)
                  .map(method -> new Object[]{isSourceOwner, method}))
            .toArray(Object[][]::new);
   }

   static Address getAddress(Cache<Object, Object> cache) {
      return cache.getAdvancedCache().getRpcManager().getAddress();
   }

   @BeforeMethod
   public void resetInvocationCount() {
      invocationCount().set(0);
   }

   @Override
   @BeforeMethod
   public void createBeforeMethod() throws Throwable {
      super.createBeforeMethod();
      this.wo = WriteOnlyMapImpl.create(fmapD1).withParams(Param.FutureMode.COMPLETED);
      this.rw = ReadWriteMapImpl.create(fmapD1).withParams(Param.FutureMode.COMPLETED);
      this.lwo = WriteOnlyMapImpl.create(fmapL1).withParams(Param.FutureMode.COMPLETED);
      this.lrw = ReadWriteMapImpl.create(fmapL1).withParams(Param.FutureMode.COMPLETED);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      countingCARDs = cacheManagers.stream().map(cm -> CountingCARD.replaceDispatcher(cm)).collect(Collectors.toList());
   }

   protected void advanceGenerationsAndAwait(long timeout, TimeUnit timeUnit) throws InterruptedException {
      long now = System.currentTimeMillis();
      long deadline = now + timeUnit.toMillis(timeout);
      for (CountingCARD card : countingCARDs) {
         card.advanceGenerationAndAwait(deadline - now, TimeUnit.MILLISECONDS);
         now = System.currentTimeMillis();
      }
   }

   protected Object getKey(boolean isSourceOwner) {
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
      return key;
   }

   protected abstract AtomicInteger invocationCount();

   protected void assertInvocations(int count) {
      assertEquals(invocationCount().get(), count);
   }

   enum Method {
      WO_EVAL(false, (key, wo, rw, read, write, invocationCountSupplier) ->
            wo.eval(key, (Consumer<WriteEntryView<String>> & Serializable) view -> {
               invocationCountSupplier.get().incrementAndGet();
               write.accept(view);
            }).join()),
      WO_EVAL_VALUE(false, (key, wo, rw, read, write, invocationCountSupplier) ->
            wo.eval(key, null, (BiConsumer<String, WriteEntryView<String>> & Serializable)
                  (v, view) -> {
                     invocationCountSupplier.get().incrementAndGet();
                     write.accept(view);
                  }).join()),
      WO_EVAL_MANY(false, (key, wo, rw, read, write, invocationCountSupplier) ->
            wo.evalMany(Collections.singleton(key), (Consumer<WriteEntryView<String>> & Serializable) view -> {
               invocationCountSupplier.get().incrementAndGet();
               write.accept(view);
            }).join()),
      WO_EVAL_MANY_ENTRIES(false, (key, wo, rw, read, write, invocationCountSupplier) ->
            wo.evalMany(Collections.singletonMap(key, null),
                  (BiConsumer<String, WriteEntryView<String>> & Serializable) (v, view) -> {
                     invocationCountSupplier.get().incrementAndGet();
                     write.accept(view);
                  }).join()),
      RW_EVAL(true, (key, wo, rw, read, write, invocationCountSupplier) ->
            rw.eval(key,
                  (Function<ReadWriteEntryView<Object, String>, Object> & Serializable) view -> {
                     invocationCountSupplier.get().incrementAndGet();
                     read.accept(view);
                     write.accept(view);
                     return null;
                  }).join()),
      RW_EVAL_VALUE(true, (key, wo, rw, read, write, invocationCountSupplier) ->
            rw.eval(key, null,
                  (BiFunction<String, ReadWriteEntryView<Object, String>, Object> & Serializable) (v, view) -> {
                     invocationCountSupplier.get().incrementAndGet();
                     read.accept(view);
                     write.accept(view);
                     return null;
                  }).join()),
      RW_EVAL_MANY(true, (key, wo, rw, read, write, invocationCountSupplier) ->
            rw.evalMany(Collections.singleton(key),
                  (Function<ReadWriteEntryView<Object, String>, Object> & Serializable) view -> {
                     invocationCountSupplier.get().incrementAndGet();
                     read.accept(view);
                     write.accept(view);
                     return null;
                  }).forEach(v -> {
            })),
      RW_EVAL_MANY_ENTRIES(true, (key, wo, rw, read, write, invocationCountSupplier) ->
            rw.evalMany(Collections.singletonMap(key, null),
                  (BiFunction<String, ReadWriteEntryView<Object, String>, Object> & Serializable) (v, view) -> {
                     invocationCountSupplier.get().incrementAndGet();
                     read.accept(view);
                     write.accept(view);
                     return null;
                  }).forEach(v -> {
            })),;

      final Performer action;
      final boolean doesRead;

      Method(boolean doesRead, Performer action) {
         this.doesRead = doesRead;
         this.action = action;
      }

      @FunctionalInterface
      interface Performer<K> {
         void eval(K key,
                   WriteOnlyMap<K, String> wo, ReadWriteMap<K, String> rw,
                   Consumer<ReadEntryView<K, String>> read, Consumer<WriteEntryView<String>> write,
                   InvocationCountSupplier invocationCountSupplier);
      }

      @FunctionalInterface
      interface InvocationCountSupplier extends Serializable, Supplier<AtomicInteger> {}
   }
}
