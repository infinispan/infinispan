package org.infinispan.manager;

import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.function.SerializableSupplier;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * @author Will Burns
 * @since 8.2
 */
@Test(groups = {"functional", "smoke"}, testName = "manager.AllClusterExecutorTest")
public class AllClusterExecutorTest extends AbstractInfinispanTest {
   static AtomicInteger atomicInteger = new AtomicInteger();

   ClusterExecutor executor(EmbeddedCacheManager cm) {
      return cm.executor();
   }

   void assertSize(EmbeddedCacheManager[] cms, int receivedSize) {
      assertEquals(cms.length, receivedSize);
   }

   void eventuallyAssertSize(EmbeddedCacheManager[] cms, Supplier<Integer> supplier) {
      eventuallyEquals(cms.length, supplier);
   }

   void assertContains(EmbeddedCacheManager[] managers, Collection<Address> results) {
      for (EmbeddedCacheManager manager : managers) {
         assertTrue(results.contains(manager.getAddress()));
      }
   }

   SerializableSupplier<AtomicInteger> atomicIntegerSupplier = () -> atomicInteger;

   public void testExecutorRunnable() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            atomicIntegerSupplier.get().set(0);
            SerializableSupplier<AtomicInteger> supplier = atomicIntegerSupplier;
            executor(cm1).submit(() -> supplier.get().getAndIncrement()).get(10, TimeUnit.SECONDS);
            assertSize(cms, atomicIntegerSupplier.get().get());
         }
      });
   }

   public void testExecutorLocalRunnable() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.LOCAL, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            atomicIntegerSupplier.get().set(0);
            SerializableSupplier<AtomicInteger> supplier = atomicIntegerSupplier;
            executor(cm1).submit(() -> supplier.get().getAndIncrement()).get(10, TimeUnit.SECONDS);
            assertSize(cms, atomicIntegerSupplier.get().get());
         }
      });
   }

   public void testExecutor3NodesRunnable() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            atomicIntegerSupplier.get().set(0);
            SerializableSupplier<AtomicInteger> supplier = atomicIntegerSupplier;
            executor(cm1).submit(() -> supplier.get().getAndIncrement()).get(10, TimeUnit.SECONDS);
            assertSize(cms, atomicIntegerSupplier.get().get());
         }
      });
   }

   public void testExecutorRunnablePredicateFilter() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            atomicIntegerSupplier.get().set(0);
            SerializableSupplier<AtomicInteger> supplier = atomicIntegerSupplier;
            executor(cm1).filterTargets(a -> a.equals(cm1.getAddress()))
                  .submit(() -> supplier.get().getAndIncrement()).get(10, TimeUnit.SECONDS);
            assertEquals(1, atomicIntegerSupplier.get().get());
         }
      });
   }

   public void testExecutorRunnableCollectionFilter() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];
            EmbeddedCacheManager cm2 = cms[1];

            atomicIntegerSupplier.get().set(0);
            SerializableSupplier<AtomicInteger> supplier = atomicIntegerSupplier;
            executor(cm1).filterTargets(Collections.singleton(cm2.getAddress()))
                  .submit(() -> supplier.get().getAndIncrement()).get(10, TimeUnit.SECONDS);
            assertEquals(1, atomicIntegerSupplier.get().get());
         }
      });
   }

   public void testExecutorRunnableException() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            CompletableFuture<Void> future = executor(cm1).submit(() -> {
               throw new TestException();
            });
            Exceptions.expectExecutionException(TestException.class, future);
         }
      });
   }

   public void testExecutorRunnableExceptionWhenComplete() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            CompletableFuture<Void> future = executor(cm1).submit(() -> {
               throw new TestException();
            });

            Exchanger<Throwable> exchanger = new Exchanger<>();
            future.whenCompleteAsync((v, t) -> {
               try {
                  exchanger.exchange(t, 10, TimeUnit.SECONDS);
               } catch (InterruptedException | TimeoutException e) {
                  throw new RuntimeException(e);
               }
            });
            Throwable t = exchanger.exchange(null, 10, TimeUnit.SECONDS);
            assertNotNull(t);
            assertEquals(TestException.class, t.getClass());
         }
      });
   }

   public void testExecutorRunnable3NodesException() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            CompletableFuture<Void> future = executor(cm1).submit(() -> {
               throw new TestException();
            });
            Exceptions.expectExecutionException(TestException.class, future);
         }
      });
   }

   public void testExecutorRunnable3NodesExceptionExcludeLocal() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            CompletableFuture<Void> future = executor(cm1).filterTargets(a -> !a.equals(cm1.getAddress())).submit(() -> {
               throw new TestException();
            });
            Exceptions.expectExecutionException(TestException.class, future);
         }
      });
   }

   public void testExecutorTimeoutException() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.DIST_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.DIST_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            CompletableFuture<Void> future = executor(cm1).timeout(10, TimeUnit.MILLISECONDS).submit(() -> {
               try {
                  Thread.sleep(1000);
               } catch (InterruptedException e) {
                  fail("Unexpected interrupt: " + e);
               }
            });
            // This fails when local node is invoked since timeout is not adhered to
            Exceptions
                  .expectExecutionException(org.infinispan.util.concurrent.TimeoutException.class, future);
         }
      });
   }


   public void testExecutorExecuteRunnable() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            atomicIntegerSupplier.get().set(0);
            SerializableSupplier<AtomicInteger> supplier = atomicIntegerSupplier;
            executor(cm1).execute(() -> supplier.get().getAndIncrement());
            eventuallyAssertSize(cms, () -> atomicIntegerSupplier.get().get());
         }
      });
   }

   public void testExecutorLocalExecuteRunnable() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.LOCAL, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            atomicIntegerSupplier.get().set(0);
            executor(cm1).execute(() -> atomicIntegerSupplier.get().getAndIncrement());
            eventuallyEquals(1, () -> atomicIntegerSupplier.get().get());
         }
      });
   }

   public void testExecutorTriConsumer() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];
            EmbeddedCacheManager cm2 = cms[1];

            AtomicReference<Throwable> throwable = new AtomicReference<>();
            List<Address> addresses = Collections.synchronizedList(new ArrayList<>(2));
            executor(cm1).submitConsumer(EmbeddedCacheManager::getAddress, (a, i, t) -> {
               if (t != null) {
                  throwable.set(t);
               } else {
                  addresses.add(i);
               }
            }).get(10, TimeUnit.SECONDS);
            Throwable t = throwable.get();
            if (t != null) {
               throw new RuntimeException(t);
            }
            assertContains(cms, addresses);
         }
      });
   }

   public void testExecutorLocalTriConsumer() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.LOCAL, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            AtomicReference<Throwable> throwable = new AtomicReference<>();
            List<Address> addresses = Collections.synchronizedList(new ArrayList<>(2));
            executor(cm1).submitConsumer(m -> m.getAddress(), (a, i, t) -> {
               if (t != null) {
                  throwable.set(t);
               } else {
                  addresses.add(i);
               }
            }).get(10, TimeUnit.SECONDS);
            Throwable t = throwable.get();
            if (t != null) {
               throw new RuntimeException(t);
            }
            assertEquals(1, addresses.size());
            assertTrue(addresses.contains(cm1.getAddress()));
         }
      });
   }

   public void testExecutor3NodeTriConsumer() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.DIST_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.DIST_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.DIST_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            AtomicReference<Throwable> throwable = new AtomicReference<>();
            List<Address> addresses = Collections.synchronizedList(new ArrayList<>(2));
            executor(cm1).submitConsumer(m -> m.getAddress(), (a, i, t) -> {
               if (t != null) {
                  throwable.set(t);
               } else {
                  addresses.add(i);
               }
            }).get(10, TimeUnit.SECONDS);
            Throwable t = throwable.get();
            if (t != null) {
               throw new RuntimeException(t);
            }
            assertContains(cms, addresses);
         }
      });
   }

   public void testExecutorTriConsumerException() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            AtomicInteger exceptionCount = new AtomicInteger();
            CompletableFuture<Void> future = executor(cm1).submitConsumer(m -> {
               throw new TestException();
            }, (a, i, t) -> {
               Exceptions.assertException(TestException.class, t);
               exceptionCount.incrementAndGet();
            });
            future.get(10, TimeUnit.SECONDS);
            assertSize(cms, exceptionCount.get());
         }
      });
   }

   public void testExecutorTriConsumerTimeoutException() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.DIST_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.DIST_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            ScheduledExecutorService stpe = Mockito.mock(ScheduledExecutorService.class, Mockito.RETURNS_DEEP_STUBS);

            for (EmbeddedCacheManager cm : cms) {
               TestingUtil.replaceComponent(cm, ScheduledExecutorService.class,
                     KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR, stpe, true);
            }

            // These are to make sure the call to the scheduled executor doesn't overlap with others, so these should
            // be unique
            long crazyNumber = 84129912895471L;
            TimeUnit unit = TimeUnit.DAYS;

            CompletableFuture<Void> future =
                  executor(cm1).timeout(crazyNumber, unit).submitConsumer(m -> {
                     ArgumentCaptor<Callable> argument = ArgumentCaptor.forClass(Callable.class);
                     // This will be a mock as we replaced them all above
                     ScheduledExecutorService innerStpe = m.getGlobalComponentRegistry().getComponent(
                           ScheduledExecutorService.class, KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR);

                     Mockito.verify(innerStpe, Mockito.timeout(TimeUnit.SECONDS.toMillis(10)).atLeastOnce()).schedule(argument.capture(),
                           Mockito.eq(crazyNumber), Mockito.eq(unit));
                     // We run the timeout ourselves, which should cause the timeout exception to occur.
                     try {
                        argument.getValue().call();
                     } catch (Exception e) {
                        throw new CacheException(e);
                     }
                     return null;
                  }, (a, i, t) -> log.tracef("Consumer invoked with %s, %s, %s", a, i, t));
            Exceptions.expectExecutionException(org.infinispan.util.concurrent.TimeoutException.class, future);
         }
      });
   }

   public void testExecutorTriConsumerExceptionFromConsumer() {
      withCacheManagers(new MultiCacheManagerCallable(
              TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
              TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            CompletableFuture<Void> future =
                    executor(cm1).submitConsumer(m -> null,
                            (a, i, t) -> {
                               throw new NullPointerException();
                            });
            Exceptions.expectExecutionException(NullPointerException.class, future);
         }
      });
   }

   public void testExecutorTriConsumerExceptionWhenComplete() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            CompletableFuture<Void> future =
                  executor(cm1).filterTargets(cm1.getAddress()::equals).submitConsumer(m -> null,
                        (a, i, t) -> {
                           throw new NullPointerException();
                        });
            Exchanger<Throwable> exchanger = new Exchanger<>();
            future.whenCompleteAsync((v, t) -> {
               try {
                  exchanger.exchange(t, 10, TimeUnit.SECONDS);
               } catch (InterruptedException | TimeoutException e) {
                  throw new RuntimeException(e);
               }
            });
            Throwable t = exchanger.exchange(null, 10, TimeUnit.SECONDS);
            assertNotNull(t);
            assertEquals(NullPointerException.class, t.getClass());
         }
      });
   }
}
