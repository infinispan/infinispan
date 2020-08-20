package org.infinispan.commons.test;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

import reactor.blockhound.BlockHound;

public class BlockHoundHelper {
   private BlockHoundHelper() { }

   /**
    * Installs BlockHound and all service loaded integrations to ensure that blocking doesn't occur where it shouldn't
    */
   static void installBlockHound() {
      // Automatically registers all services that implement BlockHoundIntegration interface

      // This is a terrible hack but gets around the issue that blockhound doesn't allow the registering thread
      // to be a dynamic blocking thread - in which case our checks in
      // AbstractInfinispanTest#currentThreadRequiresNonBlocking will never be evaluated
      // To be removed when BlockHound 1.0.4.RELEASE is available
      Thread otherThread = new Thread(BlockHound::install);
      otherThread.start();
      try {
         otherThread.join();
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Returns true if the current thread at this time requires all invocations to be non blocking
    */
   public static boolean currentThreadRequiresNonBlocking() {
      return isNonBlocking.get() == Boolean.TRUE;
   }

   private static final ThreadLocal<Boolean> isNonBlocking = ThreadLocal.withInitial(() -> Boolean.FALSE);

   /**
    * Invokes the provided Supplier in a scope that guarantees that the current thread is not blocked. If blocking is
    * found the invoking test will fail.
    */
   public static <V> V ensureNonBlocking(Supplier<V> supplier) {
      Boolean previousSetting = isNonBlocking.get();
      isNonBlocking.set(Boolean.TRUE);
      try {
         return supplier.get();
      } finally {
         isNonBlocking.set(previousSetting);
      }
   }

   /**
    * Invokes the provided Runnable in a scope that guarantees that the current thread is not blocked. If blocking is
    * found the invoking test will fail.
    */
   public static void ensureNonBlocking(Runnable runnable) {
      allowBlocking(runnable, Boolean.TRUE);
   }

   public static void allowBlocking(Runnable runnable) {
      allowBlocking(runnable, Boolean.FALSE);
   }

   private static void allowBlocking(Runnable runnable, Boolean nonBlockingSetting) {
      Boolean previousSetting = isNonBlocking.get();
      isNonBlocking.set(nonBlockingSetting);
      try {
         runnable.run();
      } finally {
         isNonBlocking.set(previousSetting);
      }
   }

   /**
    * Returns an Executor that when supplied a task, will guarantee that task does not block when invoked. If the
    * task does block it will fail the invoking test.
    */
   public static Executor ensureNonBlockingExecutor() {
      return BlockHoundHelper::ensureNonBlocking;
   }

   /**
    * Returns an Executor that when supplied a task, will allow it to invoke blocking calls, even if the invoking
    * context was already registered as non blocking. Useful to mock submission to a blocking executor.
    */
   public static Executor allowBlockingExecutor() {
      return BlockHoundHelper::allowBlocking;
   }

   /**
    * Helper method that "blocks" as dictated by block hound but in actuality does nothing. This is useful to detect
    * if the given code is actually be invoked in an method that allows for blocking or not.
    * @param consumed an argument useful for making this method a `Consumer` via method reference
    * @param <V> whatever desired type is
    */
   public static <V> void blockingConsume(V consumed) {
      // Do nothing - this method is instrumented via block hound to be "blocking" for use by tests to simulate blocking
      // without actually blocking
   }
}
