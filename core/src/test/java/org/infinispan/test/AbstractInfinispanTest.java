package org.infinispan.test;

import static org.testng.AssertJUnit.fail;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.test.ExceptionRunnable;
import org.infinispan.commons.test.TestNGLongTestsHook;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.time.TimeService;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.test.fwk.ChainMethodInterceptor;
import org.infinispan.test.fwk.FakeTestClass;
import org.infinispan.test.fwk.NamedTestMethod;
import org.infinispan.test.fwk.TestSelector;
import org.infinispan.util.EmbeddedTimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.stack.Protocol;
import org.testng.IMethodInstance;
import org.testng.IMethodInterceptor;
import org.testng.ITestContext;
import org.testng.TestNGException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.internal.MethodInstance;

import com.sun.management.UnixOperatingSystemMXBean;

import jakarta.transaction.TransactionManager;


/**
 * AbstractInfinispanTest is a superclass of all Infinispan tests.
 *
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Listeners({ChainMethodInterceptor.class, TestNGLongTestsHook.class, FeaturesListener.class})
@TestSelector(interceptors = AbstractInfinispanTest.OrderByInstance.class)
public abstract class AbstractInfinispanTest {

   @Retention(RetentionPolicy.RUNTIME)
   @Target({ElementType.TYPE})
   public @interface FeatureCondition {
      String feature();
   }

   protected interface Condition {
      boolean isSatisfied() throws Exception;
   }

   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final ThreadFactory defaultThreadFactory = getTestThreadFactory("ForkThread");
   // do not use virtual threads as most tests block/synchronize in these threads.
   private final ExecutorService testExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
               60L, TimeUnit.SECONDS,
               new SynchronousQueue<>(),
               defaultThreadFactory);
   public static final TimeService TIME_SERVICE = new EmbeddedTimeService();

   public static class OrderByInstance implements IMethodInterceptor {
      @Override
      public List<IMethodInstance> intercept(List<IMethodInstance> methods, ITestContext context) {
         Map<Object, List<IMethodInstance>> methodsByInstance = new IdentityHashMap<>();
         Map<String, Object> instancesByName = new HashMap<>();
         for (IMethodInstance method : methods) {
            methodsByInstance.computeIfAbsent(method.getInstance(), k -> new ArrayList<>()).add(method);
         }
         List<IMethodInstance> newOrder = new ArrayList<>(methods.size());
         for (Map.Entry<Object, List<IMethodInstance>> instanceAndMethods : methodsByInstance.entrySet()) {
            Object instance = instanceAndMethods.getKey();
            if (instance instanceof AbstractInfinispanTest) {
               String instanceName = ((AbstractInfinispanTest) instance).getTestName();
               Object otherInstance = instancesByName.putIfAbsent(instanceName, instance);
               if (otherInstance != null) {
                  String message = String.format("Duplicate test name: %s, classes %s and %s", instanceName,
                                                 instance.getClass().getName(), otherInstance.getClass().getName());
                  MethodInstance methodInstance =
                     FakeTestClass.newFailureMethodInstance(new TestNGException(message), context.getCurrentXmlTest(),
                                                            context, instance);
                  newOrder.add(methodInstance);
               }
               String parameters = ((AbstractInfinispanTest) instance).parameters();
               if (parameters != null) {
                  parameters = parameters.replaceAll("[-.\\+*?\\[^\\]$(){}=!<>|:\\\\]", "\\\\$0");
                  for (IMethodInstance method : instanceAndMethods.getValue()) {
                     // TestNG calls intercept twice (bug?) so this prevents adding the parameters two times
                     if (method.getMethod() instanceof NamedTestMethod) {
                        newOrder.add(method);
                     } else {
                        newOrder.add(new MethodInstance(new NamedTestMethod(method.getMethod(), method.getMethod().getMethodName() + parameters)));
                     }
                  }
                  continue;
               }
            }
            newOrder.addAll(instanceAndMethods.getValue());
         }
         return newOrder;
      }
   }

   public static String defaultParametersString(String[] names, Object[] params) {
      if (names == null || params == null) {
         return null;
      }
      assert names.length == params.length;

      boolean[] last = new boolean[params.length];
      boolean none = true;
      for (int i = params.length - 1; i >= 0; --i) {
         last[i] = none;
         none &= params[i] == null;
      }
      if (none) {
         return null;
      }
      StringBuilder sb = new StringBuilder().append('[');
      for (int i = 0; i < params.length; ++i) {
         if (params[i] != null) {
            if (names[i] != null) {
               sb.append(names[i]).append('=');
            }
            sb.append(params[i]);
            if (!last[i]) sb.append(", ");
         }
      }
      return sb.append(']').toString();
   }

   protected String parameters() {
      return null;
   }

   @BeforeClass(alwaysRun = true)
   protected void testClassStarted(ITestContext context) {
      TestResourceTracker.testStarted(getTestName());
   }

   @AfterClass(alwaysRun = true)
   protected void testClassFinished(ITestContext context) {
      killSpawnedThreads();
      nullOutFields();
      TestResourceTracker.testFinished(getTestName());
   }

   public String getTestName() {
      String className = getClass().getName();
      String parameters = parameters();
      return parameters == null ? className : className + parameters;
   }

   protected void killSpawnedThreads() {
      List<Runnable> runnables = testExecutor.shutdownNow();
      if (!runnables.isEmpty()) {
         log.errorf("There were runnables %s left uncompleted in test %s", runnables, getClass().getSimpleName());
      }
   }

   @AfterMethod(alwaysRun = true)
   public void checkOpenFDs() {
      OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
      if (os instanceof UnixOperatingSystemMXBean) {
         log.tracef("Number of open file descriptors: %d", ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount());
      }
   }

   @AfterMethod
   protected final void checkThreads() {
      if (testExecutor instanceof ThreadPoolExecutor) {
         int activeTasks = ((ThreadPoolExecutor) testExecutor).getActiveCount();
         if (activeTasks != 0) {
            log.errorf("There were %d active tasks found in the test executor service for class %s", activeTasks,
                  getClass().getSimpleName());
         }
      }
   }

   protected <T> void eventuallyEquals(T expected, Supplier<T> supplier) {
      eventually(() -> "expected:<" + expected + ">, got:<" + supplier.get() + ">",
            () -> Objects.equals(expected, supplier.get()));
   }

   protected static <T> void eventuallyEquals(String message, T expected, Supplier<T> supplier) {
      eventually(() -> message + " expected:<" + expected + ">, got:<" + supplier.get() + ">",
                 () -> Objects.equals(expected, supplier.get()));
   }

   protected static void eventually(Supplier<String> messageSupplier, Condition condition) {
      eventually(messageSupplier, condition, 30, TimeUnit.SECONDS);
   }

   protected static void eventually(Supplier<String> messageSupplier, Condition condition, long timeout,
         TimeUnit timeUnit) {
      try {
         long timeoutNanos = timeUnit.toNanos(timeout);
         // We want the sleep time to increase in arithmetic progression
         // 30 loops with the default timeout of 30 seconds means the initial wait is ~ 65 millis
         int loops = 30;
         int progressionSum = loops * (loops + 1) / 2;
         long initialSleepNanos = timeoutNanos / progressionSum;
         long sleepNanos = initialSleepNanos;
         long expectedEndTime = System.nanoTime() + timeoutNanos;
         while (expectedEndTime - System.nanoTime() > 0) {
            if (condition.isSatisfied())
               return;
            LockSupport.parkNanos(sleepNanos);
            sleepNanos += initialSleepNanos;
         }
         if (!condition.isSatisfied()) {
            fail(messageSupplier.get());
         }
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   protected static void eventually(Condition ec, long timeoutMillis) {
      eventually(ec, timeoutMillis, TimeUnit.MILLISECONDS);
   }

   protected static void eventually(Condition ec, long timeout, TimeUnit unit) {
      eventually(() -> "Condition is still false after " + timeout + " " + unit, ec, timeout, unit);
   }

   protected void eventually(String message, Condition ec, long timeout, TimeUnit unit) {
      eventually(() -> message, ec, unit.toMillis(timeout), TimeUnit.MILLISECONDS);
   }

   /**
    * This method will actually spawn a fresh thread and will not use underlying pool.  The
    * {@link org.infinispan.test.AbstractInfinispanTest#fork(ExceptionRunnable)} should be preferred
    * unless you require explicit access to the thread.
    *
    * @param r The runnable to run
    * @return The created thread
    */
   protected Thread inNewThread(Runnable r) {
      final Thread t = defaultThreadFactory.newThread(new RunnableWrapper(r));
      log.tracef("About to start thread '%s' as child of thread '%s'", t.getName(), Thread.currentThread().getName());
      t.start();
      return t;
   }

   protected Future<Void> fork(ExceptionRunnable r) {
      return testExecutor.submit(new CallableWrapper<>(() -> {
         r.run();
         return null;
      }));
   }

   protected <T> Future<T> fork(Callable<T> c) {
      return testExecutor.submit(new CallableWrapper<>(c));
   }

   /**
    * This should normally not be used, use the {@code fork(Runnable|Callable|ExceptionRunnable)}
    * method when an executor is required.
    *
    * Although if you want a limited set of threads this could still be useful for something like
    * {@link java.util.concurrent.Executors#newFixedThreadPool(int, java.util.concurrent.ThreadFactory)} or
    * {@link java.util.concurrent.Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)}
    *
    * @param prefix The prefix starting for the thread factory
    * @return A thread factory that will use the same naming schema as the other methods
    */
   protected ThreadFactory getTestThreadFactory(final String prefix) {
      final String className = getClass().getSimpleName();

      return new ThreadFactory() {
         private final AtomicInteger counter = new AtomicInteger(0);

         @Override
         public Thread newThread(Runnable r) {
            String threadName = prefix + "-" + counter.incrementAndGet() + "," + className;
            Thread thread = new Thread(r, threadName);
            TestResourceTracker.addResource(AbstractInfinispanTest.this.getTestName(), new ThreadCleaner(thread));
            return thread;
         }
      };
   }

   /**
    * This will run two or more tasks concurrently.
    *
    * It synchronizes before starting at approximately the same time by ensuring they all start before
    * allowing the tasks to proceed.
    *
    * @param tasks The tasks to run
    * @throws InterruptedException Thrown if this thread is interrupted
    * @throws ExecutionException Thrown if one of the callables throws any kind of Throwable.  The
    *         thrown Throwable will be wrapped by this exception
    * @throws TimeoutException If one of the tasks doesn't complete within the timeout
    */
   protected void runConcurrently(long timeout, TimeUnit timeUnit, ExceptionRunnable... tasks) throws Exception {
      if (tasks == null || tasks.length < 2) {
         throw new IllegalArgumentException("Need at least 2 tasks to run concurrently");
      }

      long deadlineNanos = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
      List<Future<Void>> futures = new ArrayList<>(tasks.length);
      CyclicBarrier barrier = new CyclicBarrier(tasks.length);
      for (ExceptionRunnable task : tasks) {
         futures.add(testExecutor.submit(new ConcurrentCallable(task, barrier)));
      }

      List<Exception> exceptions = new ArrayList<>();
      for (Future<Void> future : futures) {
         try {
            future.get(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
         } catch (Exception e) {
            futures.forEach(f -> f.cancel(true));
            exceptions.add(e);
         }
      }

      if (!exceptions.isEmpty()) {
         Exception exception = exceptions.remove(0);
         for (Exception e : exceptions) {
            exception.addSuppressed(e);
         }
         throw exception;
      }
   }

   /**
    * This will run two or more tasks concurrently.
    *
    * It synchronizes before starting at approximately the same time by ensuring they all start before
    * allowing the tasks to proceed.
    *
    * @param tasks The tasks to run
    * @throws InterruptedException Thrown if this thread is interrupted
    * @throws ExecutionException Thrown if one of the callables throws any kind of Throwable.  The
    *         thrown Throwable will be wrapped by this exception
    * @throws TimeoutException If one of the tasks doesn't complete within the timeout
    */
   protected void runConcurrently(long timeout, TimeUnit timeUnit, Callable<?>... tasks) throws Exception {
      runConcurrently(timeout, timeUnit,
                      Arrays.stream(tasks).<ExceptionRunnable>map(task -> task::call)
                         .toArray(ExceptionRunnable[]::new));
   }

   /**
    * Equivalent to {@code runConcurrently(30, SECONDS, tasks)}
    */
   protected void runConcurrently(ExceptionRunnable... tasks) throws Exception {
      runConcurrently(30, TimeUnit.SECONDS, tasks);
   }

   /**
    * Equivalent to {@code runConcurrently(30, SECONDS, tasks)}
    */
   protected void runConcurrently(Callable<?>... tasks) throws Exception {
      runConcurrently(
         Arrays.stream(tasks).<ExceptionRunnable>map(task -> task::call).toArray(ExceptionRunnable[]::new));
   }

   protected static void eventually(Condition ec) {
      eventually(ec, 10000, TimeUnit.MILLISECONDS);
   }

   protected void eventually(String message, Condition ec) {
      eventually(message, ec, 10000, TimeUnit.MILLISECONDS);
   }

   public void safeRollback(TransactionManager transactionManager) {
      try {
         transactionManager.rollback();
      } catch (Exception e) {
         //ignored
      }
   }


   protected void nullOutFields() {
      // TestNG keeps test instances in memory forever, make them leaner by clearing direct references to caches
      for (Field field : getAllFields()) {
         if (!Modifier.isFinal(field.getModifiers()) && fieldIsMemoryHog(field)) {
            field.setAccessible(true);
            try {
               field.set(this, null);
            } catch (IllegalArgumentException | IllegalAccessException e) {
               log.error(e);
            }
         }
      }
   }

   private boolean fieldIsMemoryHog(Field field) {
      Class<?>[] memoryHogs = {BasicCacheContainer.class, BasicCache.class, FunctionalMap.class, Protocol.class,
                               AsyncInterceptor.class, RequestRepository.class,
                               BasePartitionHandlingTest.Partition.class};
      return Stream.of(memoryHogs).anyMatch(clazz -> fieldIsMemoryHog(field, clazz));
   }

   private boolean fieldIsMemoryHog(Field field, Class<?> clazz) {
      if (clazz.isAssignableFrom(field.getType())) {
         return true;
      } else if (field.getType().isArray()) {
         return (clazz.isAssignableFrom(field.getType().getComponentType()));
      } else if (Collection.class.isAssignableFrom(field.getType())) {
         Type fieldType = field.getGenericType();
         if (fieldType instanceof Class<?>) {
            return clazz.isAssignableFrom((Class<?>) fieldType);
         }
         if (fieldType instanceof ParameterizedType) {
            ParameterizedType collectionType = (ParameterizedType) fieldType;
            Type elementType = collectionType.getActualTypeArguments()[0];
            if (elementType instanceof ParameterizedType) {
               return clazz.isAssignableFrom(((Class<?>) ((ParameterizedType) elementType).getRawType()));
            } else if (elementType instanceof Class<?>) {
               return clazz.isAssignableFrom(((Class<?>) elementType));
            }
         }
         return false;
      } else {
         return false;
      }
   }

   private Collection<Field> getAllFields() {
      Collection<Field> fields = new ArrayList<>();
      Class<?> clazz = this.getClass();
      while (clazz != null) {
         fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
         clazz = clazz.getSuperclass();
      }
      return fields;
   }

   protected ExecutorService testExecutor() {
      return testExecutor;
   }

   private static class ThreadCleaner extends TestResourceTracker.Cleaner<Thread> {
      public ThreadCleaner(Thread thread) {
         super(thread);
      }

      @Override
      public void close() {
         if (ref.isAlive() && !ref.isInterrupted()) {
            log.warnf("There was a thread %s still alive after test completion - interrupted it",
                      ref);
            ref.interrupt();
         }
      }
   }

   /**
    * A callable that will first await on the provided barrier before calling the provided callable.
    * This is useful to have a better attempt at multiple threads ran at the same time, but still is
    * no guarantee since this is controlled by the thread scheduler.
    */
   public static final class ConcurrentCallable implements Callable<Void> {
      private final ExceptionRunnable task;
      private final CyclicBarrier barrier;

      ConcurrentCallable(ExceptionRunnable task, CyclicBarrier barrier) {
         this.task = task;
         this.barrier = barrier;
      }

      @Override
      public Void call() throws Exception {
         try {
            log.trace("Started concurrent callable");
            barrier.await(10, TimeUnit.SECONDS);
            log.trace("Synchronized with the other concurrent runnables");
            task.run();
            log.debug("Exiting fork runnable.");
            return null;
         } catch (Throwable e) {
            log.warn("Exiting fork runnable due to exception", e);
            throw e;
         }
      }
   }

   public static final class RunnableWrapper implements Runnable {
      final Runnable realOne;

      RunnableWrapper(Runnable realOne) {
         this.realOne = realOne;
      }

      @Override
      public void run() {
         try {
            log.trace("Started fork runnable..");
            realOne.run();
            log.debug("Exiting fork runnable.");
         } catch (Throwable e) {
            log.warn("Exiting fork runnable due to exception", e);
            throw e;
         }
      }
   }

   private static class CallableWrapper<T> implements Callable<T> {
      private final Callable<? extends T> c;

      CallableWrapper(Callable<? extends T> c) {
         this.c = c;
      }

      @Override
      public T call() throws Exception {
         try {
            log.trace("Started fork callable..");
            T result = c.call();
            log.debug("Exiting fork callable.");
            return result;
         } catch (Exception e) {
            log.warn("Exiting fork callable due to exception", e);
            throw e;
         }
      }
   }
}
