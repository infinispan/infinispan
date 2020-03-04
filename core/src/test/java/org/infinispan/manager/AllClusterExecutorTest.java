package org.infinispan.manager;

import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestClassLocal;
import org.infinispan.util.function.SerializableFunction;
import org.infinispan.util.function.SerializableSupplier;
import org.testng.annotations.Test;

/**
 * @author Will Burns
 * @since 8.2
 */
@Test(groups = {"functional", "smoke"}, testName = "manager.AllClusterExecutorTest")
public class AllClusterExecutorTest extends AbstractInfinispanTest {
   static AtomicInteger atomicInteger = new AtomicInteger();
   private TestClassLocal<CheckPoint> checkPoint = new TestClassLocal<>("checkpoint", this, CheckPoint::new, c -> {});

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
            }, TestingUtil.extractGlobalComponent(cm1, ExecutorService.class, KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR));
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
            TestClassLocal<CheckPoint> checkPoint = AllClusterExecutorTest.this.checkPoint;

            CompletableFuture<Void> future = executor(cm1).timeout(1, TimeUnit.MILLISECONDS).submit(() -> {
               try {
                  checkPoint.get().awaitStrict("resume_remote_execution", 10, TimeUnit.SECONDS);
               } catch (InterruptedException | TimeoutException e) {
                  throw new TestException(e);
               }
            });
            // This fails when local node is invoked since timeout is not adhered to
            Exceptions.expectExecutionException(org.infinispan.util.concurrent.TimeoutException.class, future);

            checkPoint.get().trigger("resume_remote_execution");
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
            TestClassLocal<CheckPoint> checkPoint = AllClusterExecutorTest.this.checkPoint;
            SerializableFunction<EmbeddedCacheManager, Object> blockingFunction = m -> {
               try {
                  checkPoint.get().trigger("block_execution");
                  checkPoint.get().awaitStrict("resume_execution", 10, TimeUnit.SECONDS);
                  checkPoint.get().trigger("complete");
               } catch (InterruptedException | TimeoutException e) {
                  throw new TestException(e);
               }
               return null;
            };

            CompletableFuture<Void> futureRemote =
               executor(cm1).filterTargets(a -> !a.equals(cm1.getAddress()))
                            .timeout(1, TimeUnit.MILLISECONDS)
                            .submitConsumer(blockingFunction, (a, i, t) -> {
                               log.tracef("Consumer invoked with %s, %s, %s", a, i, t);
                            });
            Exceptions.expectExecutionException(org.infinispan.util.concurrent.TimeoutException.class, futureRemote);
            checkPoint.get().awaitStrict("block_execution", 10, TimeUnit.SECONDS);
            checkPoint.get().trigger("resume_execution");
            // Have to wait for callback to complete - otherwise a different thread could find the "resume_execution"
            // checkpoint reached incorrectly
            checkPoint.get().awaitStrict("complete", 10, TimeUnit.SECONDS);

            CompletableFuture<Void> futureLocal =
               executor(cm1).filterTargets(a -> a.equals(cm1.getAddress()))
                            .timeout(1, TimeUnit.MILLISECONDS)
                            .submitConsumer(blockingFunction, (a, i, t) -> {
                               log.tracef("Consumer invoked with %s, %s, %s", a, i, t);
                            });
            Exceptions.expectExecutionException(org.infinispan.util.concurrent.TimeoutException.class, futureLocal);
            checkPoint.get().awaitStrict("block_execution", 10, TimeUnit.SECONDS);
            checkPoint.get().trigger("resume_execution");
            checkPoint.get().awaitStrict("complete", 10, TimeUnit.SECONDS);
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
            }, TestingUtil.extractGlobalComponent(cm1, ExecutorService.class, KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR));
            Throwable t = exchanger.exchange(null, 10, TimeUnit.SECONDS);
            assertNotNull(t);
            assertEquals(NullPointerException.class, t.getClass());
         }
      });
   }
}
