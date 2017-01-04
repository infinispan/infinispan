package org.infinispan.functional;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.TxReadOnlyKeyCommand;
import org.infinispan.commands.functional.TxReadOnlyManyCommand;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.commons.api.functional.FunctionalMap;
import org.infinispan.commons.api.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.commons.api.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.impl.CallInterceptor;
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
   static ConcurrentMap<Class<? extends AbstractFunctionalOpTest>, AtomicInteger> invocationCounts = new ConcurrentHashMap<>();

   FunctionalMap.ReadOnlyMap<Object, String> ro;
   FunctionalMap.ReadOnlyMap<Integer, String> lro;
   WriteOnlyMap<Object, String> wo;
   ReadWriteMap<Object, String> rw;
   WriteOnlyMap<Integer, String> lwo;
   ReadWriteMap<Integer, String> lrw;
   List<CountingCARD> countingCARDs;

   public AbstractFunctionalOpTest() {
      numNodes = 4;
      numDistOwners = 2;
   }

   @DataProvider(name = "writeMethods")
   public static Object[][] writeMethods() {
      return Stream.of(WriteMethod.values()).map(m -> new Object[]{m}).toArray(Object[][]::new);
   }

   @DataProvider(name = "owningModeAndWriteMethod")
   public static Object[][] owningModeAndWriteMethod() {
      return Stream.of(Boolean.TRUE, Boolean.FALSE)
            .flatMap(isSourceOwner -> Stream.of(WriteMethod.values())
                  .map(method -> new Object[]{isSourceOwner, method}))
            .toArray(Object[][]::new);
   }

   @DataProvider(name = "readMethods")
   public static Object[][] methods() {
      return Stream.of(ReadMethod.values()).map(m -> new Object[] { m }).toArray(Object[][]::new);
   }

   @DataProvider(name = "owningModeAndReadMethod")
   public static Object[][] owningModeAndMethod() {
      return Stream.of(Boolean.TRUE, Boolean.FALSE)
            .flatMap(isSourceOwner -> Stream.of(ReadMethod.values())
                  .map(method -> new Object[] { isSourceOwner, method }))
            .toArray(Object[][]::new);
   }

   @DataProvider(name = "owningModeAndReadWrites")
   public static Object[][] owningModeAndReadWrites() {
      return Stream.of(Boolean.TRUE, Boolean.FALSE)
            .flatMap(isSourceOwner -> Stream.of(WriteMethod.values()).filter(m -> m.doesRead)
                  .map(method -> new Object[]{isSourceOwner, method}))
            .toArray(Object[][]::new);
   }

   static Address getAddress(Cache<Object, Object> cache) {
      return cache.getAdvancedCache().getRpcManager().getAddress();
   }

   @BeforeMethod
   public void resetInvocationCount() {
      AtomicInteger counter = invocationCounts.get(getClass());
      if (counter != null) counter.set(0);
   }

   @Override
   @BeforeMethod
   public void createBeforeMethod() throws Throwable {
      super.createBeforeMethod();
      this.ro = ReadOnlyMapImpl.create(fmapD1).withParams(Param.FutureMode.COMPLETED);
      this.lro = ReadOnlyMapImpl.create(fmapL1).withParams(Param.FutureMode.COMPLETED);
      this.wo = WriteOnlyMapImpl.create(fmapD1).withParams(Param.FutureMode.COMPLETED);
      this.rw = ReadWriteMapImpl.create(fmapD1).withParams(Param.FutureMode.COMPLETED);
      this.lwo = WriteOnlyMapImpl.create(fmapL1).withParams(Param.FutureMode.COMPLETED);
      this.lrw = ReadWriteMapImpl.create(fmapL1).withParams(Param.FutureMode.COMPLETED);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      countingCARDs = cacheManagers.stream().map(cm -> CountingCARD.replaceDispatcher(cm)).collect(Collectors.toList());
      Stream.of(null, DIST, REPL).forEach(name -> caches(name).forEach(c -> {
         c.getAdvancedCache().getAsyncInterceptorChain().addInterceptorBefore(new CommandCachingInterceptor(), CallInterceptor.class);
      }));
   }

   protected void advanceGenerationsAndAwait(long timeout, TimeUnit timeUnit) throws InterruptedException {
      long now = System.currentTimeMillis();
      long deadline = now + timeUnit.toMillis(timeout);
      for (CountingCARD card : countingCARDs) {
         card.advanceGenerationAndAwait(deadline - now, TimeUnit.MILLISECONDS);
         now = System.currentTimeMillis();
      }
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

   protected void assertInvocations(int expected) {
      AtomicInteger counter = invocationCounts.get(getClass());
      assertEquals(counter == null ? 0 : counter.get(), expected);
   }

   // we don't want to increment the invocation count if it's the non-modifying invocation during transactional read-writes
   private static boolean isModifying() {
      VisitableCommand current = CommandCachingInterceptor.getCurrent();
      return !(current instanceof TxReadOnlyKeyCommand || current instanceof TxReadOnlyManyCommand);
   }

   private static void incrementInvocationCount(Class<? extends AbstractFunctionalOpTest> clazz) {
      invocationCounts.computeIfAbsent(clazz, k -> new AtomicInteger()).incrementAndGet();
   }

   enum WriteMethod {
      WO_EVAL(false, (key, wo, rw, read, write, clazz) ->
            wo.eval(key, (Consumer<WriteEntryView<String>> & Serializable) view -> {
               if (isModifying()) {
                  incrementInvocationCount(clazz);
               }
               write.accept(view, null);
            }).join()),
      WO_EVAL_VALUE(false, (key, wo, rw, read, write, clazz) ->
            wo.eval(key, null, (BiConsumer<String, WriteEntryView<String>> & Serializable)
                  (v, view) -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     write.accept(view, null);
                  }).join()),
      WO_EVAL_MANY(false, (key, wo, rw, read, write, clazz) ->
            wo.evalMany(Collections.singleton(key), (Consumer<WriteEntryView<String>> & Serializable) view -> {
               if (isModifying()) {
                  incrementInvocationCount(clazz);
               }
               write.accept(view, null);
            }).join()),
      WO_EVAL_MANY_ENTRIES(false, (key, wo, rw, read, write, clazz) ->
            wo.evalMany(Collections.singletonMap(key, null),
                  (BiConsumer<String, WriteEntryView<String>> & Serializable) (v, view) -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     write.accept(view, null);
                  }).join()),
      RW_EVAL(true, (key, wo, rw, read, write, clazz) ->
            rw.eval(key,
                  (Function<ReadWriteEntryView<Object, String>, Object> & Serializable) view -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     Object ret = read.apply(view);
                     write.accept(view, ret);
                     return ret;
                  }).join()),
      RW_EVAL_VALUE(true, (key, wo, rw, read, write, clazz) ->
            rw.eval(key, null,
                  (BiFunction<String, ReadWriteEntryView<Object, String>, Object> & Serializable) (v, view) -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     Object ret = read.apply(view);
                     write.accept(view, ret);
                     return ret;
                  }).join()),
      RW_EVAL_MANY(true, (key, wo, rw, read, write, clazz) ->
            rw.evalMany(Collections.singleton(key),
                  (Function<ReadWriteEntryView<Object, String>, Object> & Serializable) view -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     Object ret = read.apply(view);
                     write.accept(view, ret);
                     return ret;
                  }).filter(Objects::nonNull).findAny().orElse(null)),
      RW_EVAL_MANY_ENTRIES(true, (key, wo, rw, read, write, clazz) ->
            rw.evalMany(Collections.singletonMap(key, null),
                  (BiFunction<String, ReadWriteEntryView<Object, String>, Object> & Serializable) (v, view) -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     Object ret = read.apply(view);
                     write.accept(view, ret);
                     return ret;
                  }).filter(Objects::nonNull).findAny().orElse(null)),;

      final Performer action;
      final boolean doesRead;

      WriteMethod(boolean doesRead, Performer action) {
         this.doesRead = doesRead;
         this.action = action;
      }

      @FunctionalInterface
      interface Performer<K, R> {
         R eval(K key,
                WriteOnlyMap<K, String> wo, ReadWriteMap<K, String> rw,
                Function<ReadEntryView<K, String>, R> read, BiConsumer<WriteEntryView<String>, R> write,
                Class<? extends AbstractFunctionalOpTest> clazz);
      }
   }

   enum ReadMethod {
      RO_EVAL((key, ro, read) -> ro.eval(key, read).join()),
      RO_EVAL_MANY((key, ro, read) -> ro.evalMany(Collections.singleton(key), read).filter(Objects::nonNull).findAny().orElse(null)),
      ;

      final Performer action;

      ReadMethod(Performer action) {
         this.action = action;
      }

      @FunctionalInterface
      interface Performer<K, R> {
         R eval(K key,
                   FunctionalMap.ReadOnlyMap<K, String> ro,
                   Function<ReadEntryView<Object, String>, R> read);
      }
   }

   protected static class CommandCachingInterceptor extends BaseCustomAsyncInterceptor {
      private static ThreadLocal<VisitableCommand> current = new ThreadLocal<>();

      public static VisitableCommand getCurrent() {
         return current.get();
      }

      @Override
      protected InvocationStage handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         current.set(command);
         return invokeNext(ctx, command).whenComplete(ctx, command, (rCtx, rCommand, rv, t) -> current.remove());
      }
   }
}
