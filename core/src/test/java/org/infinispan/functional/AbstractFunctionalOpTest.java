package org.infinispan.functional;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.TxReadOnlyKeyCommand;
import org.infinispan.commands.functional.TxReadOnlyManyCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.context.InvocationContext;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.CountingRequestRepository;
import org.infinispan.util.function.SerializableBiConsumer;
import org.infinispan.util.function.SerializableFunction;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.infinispan.test.Exceptions.expectExceptionNonStrict;
import static org.testng.Assert.assertEquals;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; && Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.AbstractFunctionalOpTest")
public abstract class AbstractFunctionalOpTest extends AbstractFunctionalTest {
   static ConcurrentMap<Class<? extends AbstractFunctionalOpTest>, AtomicInteger> invocationCounts = new ConcurrentHashMap<>();

   FunctionalMap.ReadOnlyMap<Object, String> ro;
   FunctionalMap.ReadOnlyMap<Object, String> sro;
   FunctionalMap.ReadOnlyMap<Integer, String> lro;
   WriteOnlyMap<Object, String> wo;
   ReadWriteMap<Object, String> rw;
   AdvancedCache<Object, String> cache;
   WriteOnlyMap<Object, String> swo;
   ReadWriteMap<Object, String> srw;
   AdvancedCache<Object, String> scatteredCache;
   WriteOnlyMap<Integer, String> lwo;
   ReadWriteMap<Integer, String> lrw;
   List<CountingRequestRepository> countingRequestRepositories;

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
      this.ro = ReadOnlyMapImpl.create(fmapD1);
      this.sro = ReadOnlyMapImpl.create(fmapS1);
      this.lro = ReadOnlyMapImpl.create(fmapL1);
      this.wo = WriteOnlyMapImpl.create(fmapD1);
      this.rw = ReadWriteMapImpl.create(fmapD1);
      this.cache = cacheManagers.get(0).<Object, String>getCache(DIST).getAdvancedCache();
      this.swo = WriteOnlyMapImpl.create(fmapS1);
      this.srw = ReadWriteMapImpl.create(fmapS1);
      this.scatteredCache = cacheManagers.get(0).<Object, String>getCache(SCATTERED).getAdvancedCache();
      this.lwo = WriteOnlyMapImpl.create(fmapL1);
      this.lrw = ReadWriteMapImpl.create(fmapL1);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      countingRequestRepositories = cacheManagers.stream().map(cm -> CountingRequestRepository.replaceDispatcher(cm)).collect(Collectors.toList());
      Stream.of(null, DIST, REPL, SCATTERED).forEach(name -> caches(name).forEach(c -> {
         c.getAdvancedCache().getAsyncInterceptorChain().addInterceptorBefore(new CommandCachingInterceptor(), CallInterceptor.class);
      }));
   }

   protected void advanceGenerationsAndAwait(long timeout, TimeUnit timeUnit) throws Exception {
      long now = System.currentTimeMillis();
      long deadline = now + timeUnit.toMillis(timeout);
      for (CountingRequestRepository card : countingRequestRepositories) {
         card.advanceGenerationAndAwait(deadline - now, TimeUnit.MILLISECONDS);
         now = System.currentTimeMillis();
      }
   }

   protected Object getKey(boolean isOwner, String cacheName) {
      Object key;
      if (isOwner) {
         // this is simple: find a key that is local to the originating node
         key = getKeyForCache(0, cacheName);
      } else {
         // this is more complicated: we need a key that is *not* local to the originating node
         key = IntStream.iterate(0, i -> i + 1)
               .mapToObj(i -> "key" + i)
               .filter(k -> !cache(0, cacheName).getAdvancedCache().getDistributionManager().getLocality(k).isLocal())
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

   protected <K> void testReadOnMissingValue(K key, FunctionalMap.ReadOnlyMap<K, String> ro, ReadMethod method) {
      assertEquals(ro.eval(key, view -> view.find().isPresent()).join(), Boolean.FALSE);
      expectExceptionNonStrict(CompletionException.class, CacheException.class, NoSuchElementException.class, () ->
            method.eval(key, ro, view -> view.get())
      );
   }

   enum WriteMethod {
      WO_EVAL(false, (key, wo, rw, read, write, clazz) ->
            wo.eval(key, view -> {
               if (isModifying()) {
                  incrementInvocationCount(clazz);
               }
               write.accept(view, null);
            }).join(), false),
      WO_EVAL_VALUE(false, (key, wo, rw, read, write, clazz) ->
            wo.eval(key, null, (v, view) -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     write.accept(view, null);
                  }).join(), false),
      WO_EVAL_MANY(false, (key, wo, rw, read, write, clazz) ->
            wo.evalMany(Collections.singleton(key), view -> {
               if (isModifying()) {
                  incrementInvocationCount(clazz);
               }
               write.accept(view, null);
            }).join(), true),
      WO_EVAL_MANY_ENTRIES(false, (key, wo, rw, read, write, clazz) ->
            wo.evalMany(Collections.singletonMap(key, null), (v, view) -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     write.accept(view, null);
                  }).join(), true),
      RW_EVAL(true, (key, wo, rw, read, write, clazz) ->
            rw.eval(key, view -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     Object ret = read.apply(view);
                     write.accept(view, ret);
                     return ret;
                  }).join(), false),
      RW_EVAL_VALUE(true, (key, wo, rw, read, write, clazz) ->
            rw.eval(key, null, (v, view) -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     Object ret = read.apply(view);
                     write.accept(view, ret);
                     return ret;
                  }).join(), false),
      RW_EVAL_MANY(true, (key, wo, rw, read, write, clazz) ->
            rw.evalMany(Collections.singleton(key), view -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     Object ret = read.apply(view);
                     write.accept(view, ret);
                     return ret;
                  }).filter(Objects::nonNull).findAny().orElse(null), true),
      RW_EVAL_MANY_ENTRIES(true, (key, wo, rw, read, write, clazz) ->
            rw.evalMany(Collections.singletonMap(key, null), (v, view) -> {
                     if (isModifying()) {
                        incrementInvocationCount(clazz);
                     }
                     Object ret = read.apply(view);
                     write.accept(view, ret);
                     return ret;
                  }).filter(Objects::nonNull).findAny().orElse(null), true),;

      private final Performer action;
      final boolean doesRead;
      final boolean isMany;

      <K, R> WriteMethod(boolean doesRead, Performer<K, R> action, boolean isMany) {
         this.doesRead = doesRead;
         this.action = action;
         this.isMany = isMany;
      }

      public <K, R> R eval(K key,
                           WriteOnlyMap<K, String> wo, ReadWriteMap<K, String> rw,
                           SerializableFunction<ReadEntryView<K, String>, R> read,
                           SerializableBiConsumer<WriteEntryView<String>, R> write,
                           Class<? extends AbstractFunctionalOpTest> clazz) {
         return ((Performer<K, R>) action).eval(key, wo, rw, read, write, clazz);
      }

      public <K, R> R eval(K key,
                           WriteOnlyMap<K, String> wo, ReadWriteMap<K, String> rw,
                           Function<ReadEntryView<K, String>, R> read,
                           SerializableBiConsumer<WriteEntryView<String>, R> write,
                           Class<? extends AbstractFunctionalOpTest> clazz) {
         return ((Performer<K, R>) action).eval(key, wo, rw, read, write, clazz);
      }

      @FunctionalInterface
      private interface Performer<K, R> {
         R eval(K key,
                WriteOnlyMap<K, String> wo, ReadWriteMap<K, String> rw,
                Function<ReadEntryView<K, String>, R> read,
                BiConsumer<WriteEntryView<String>, R> write,
                Class<? extends AbstractFunctionalOpTest> clazz);
      }
   }

   enum ReadMethod {
      RO_EVAL((key, ro, read) -> ro.eval(key, read).join()),
      RO_EVAL_MANY((key, ro, read) -> ro.evalMany(Collections.singleton(key), read).filter(Objects::nonNull).findAny().orElse(null)),
      ;

      private final Performer action;

      ReadMethod(Performer action) {
         this.action = action;
      }

      public <K, R> R eval(K key, FunctionalMap.ReadOnlyMap<K, String> ro,
                           SerializableFunction<ReadEntryView<K, String>, R> read) {
         return ((Performer<K, R>) action).eval(key, ro, read);
      }

      public <K, R> R eval(K key, FunctionalMap.ReadOnlyMap<K, String> ro,
                           Function<ReadEntryView<K, String>, R> read) {
         return ((Performer<K, R>) action).eval(key, ro, read);
      }

      @FunctionalInterface
      private interface Performer<K, R> {
         R eval(K key,
                   FunctionalMap.ReadOnlyMap<K, String> ro,
                   Function<ReadEntryView<K, String>, R> read);
      }
   }

   protected static class CommandCachingInterceptor extends BaseCustomAsyncInterceptor {
      private static ThreadLocal<VisitableCommand> current = new ThreadLocal<>();

      public static VisitableCommand getCurrent() {
         return current.get();
      }

      @Override
      protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         current.set(command);
         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> current.remove());
      }
   }
}
