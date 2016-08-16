package org.infinispan.manager;

import static org.infinispan.test.TestingUtil.withCacheManagers;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultiCacheManagerCallable;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Will Burns
 * @since 8.2
 */
@Test(groups = {"functional", "smoke"}, testName = "manager.ClusterExecutorTest")
public class ClusterExecutorTest extends AbstractInfinispanTest {
   static AtomicInteger atomicInteger = new AtomicInteger();

   public void testExecutorRunnable() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            atomicInteger.set(0);
            cm1.executor().submit(() -> atomicInteger.getAndIncrement()).get(10, TimeUnit.SECONDS);
            assertEquals(2, atomicInteger.get());
         }
      });
   }

   public void testExecutorLocalRunnable() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.LOCAL, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            atomicInteger.set(0);
            cm1.executor().submit(() -> atomicInteger.getAndIncrement()).get(10, TimeUnit.SECONDS);
            assertEquals(1, atomicInteger.get());
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

            atomicInteger.set(0);
            cm1.executor().submit(() -> atomicInteger.getAndIncrement()).get(10, TimeUnit.SECONDS);
            assertEquals(3, atomicInteger.get());
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

            atomicInteger.set(0);
            cm1.executor().filterTargets(a -> a.equals(cm1.getAddress()))
                  .submit(() -> atomicInteger.getAndIncrement()).get(10, TimeUnit.SECONDS);
            assertEquals(1, atomicInteger.get());
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

            atomicInteger.set(0);
            cm1.executor().filterTargets(Collections.singleton(cm2.getAddress()))
                  .submit(() -> atomicInteger.getAndIncrement()).get(10, TimeUnit.SECONDS);
            assertEquals(1, atomicInteger.get());
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

            CompletableFuture<Void> future = cm1.executor().submit(() -> {
               throw new TestException();
            });
            Exceptions.expectExecutionException(TestException.class, future);
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

            CompletableFuture<Void> future = cm1.executor().submit(() -> {
               throw new TestException();
            });
            Exceptions.expectExecutionException(TestException.class, future);
         }
      });
   }

   public void testExecutorTimeoutException() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            CompletableFuture<Void> future = cm1.executor().timeout(10, TimeUnit.MILLISECONDS).submit(() -> {
               try {
                  Thread.sleep(100);
               } catch (InterruptedException e) {
                  fail("Unexpected interrupt: " + e);
               }
            });
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

            atomicInteger.set(0);
            cm1.executor().execute(() -> atomicInteger.getAndIncrement());
            eventually(() -> 2 == atomicInteger.get());
         }
      });
   }

   public void testExecutorLocalExecuteRunnable() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.LOCAL, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            atomicInteger.set(0);
            cm1.executor().execute(() -> atomicInteger.getAndIncrement());
            eventually(() -> 1 == atomicInteger.get());
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
            cm1.executor().submitConsumer(EmbeddedCacheManager::getAddress, (a, i, t) -> {
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
            assertEquals(2, addresses.size());
            assertTrue(addresses.contains(cm1.getAddress()));
            assertTrue(addresses.contains(cm2.getAddress()));
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
            cm1.executor().submitConsumer(m -> m.getAddress(), (a, i, t) -> {
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
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];
            EmbeddedCacheManager cm2 = cms[1];
            EmbeddedCacheManager cm3 = cms[2];

            AtomicReference<Throwable> throwable = new AtomicReference<>();
            List<Address> addresses = Collections.synchronizedList(new ArrayList<>(2));
            cm1.executor().submitConsumer(m -> m.getAddress(), (a, i, t) -> {
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
            assertTrue(addresses.contains(cm1.getAddress()));
            assertTrue(addresses.contains(cm2.getAddress()));
            assertTrue(addresses.contains(cm3.getAddress()));
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
            CompletableFuture<Void> future = cm1.executor().submitConsumer(m -> {
               throw new TestException();
            }, (a, i, t) -> {
               assertTrue(t instanceof CompletionException || t instanceof CacheException);
               Exceptions.assertException(TestException.class, t.getCause());
               exceptionCount.incrementAndGet();
            });
            future.get(10, TimeUnit.SECONDS);
            assertEquals(2, exceptionCount.get());
         }
      });
   }

   public void testExecutorTriConsumerTimeoutException() {
      withCacheManagers(new MultiCacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false),
            TestCacheManagerFactory.createCacheManager(CacheMode.REPL_SYNC, false)) {
         @Override
         public void call() throws InterruptedException, ExecutionException, TimeoutException {
            EmbeddedCacheManager cm1 = cms[0];

            CompletableFuture<Void> future =
                  cm1.executor().timeout(10, TimeUnit.MILLISECONDS).submitConsumer(m -> {
                     TestingUtil.sleepThread(100);
                     return null;
                  }, (a, i, t) -> { });
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
                    cm1.executor().submitConsumer(m -> null,
                            (a, i, t) -> {
                               throw new NullPointerException();
                            });
            Exceptions.expectExecutionException(NullPointerException.class, future);
         }
      });
   }
}
