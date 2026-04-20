package org.infinispan.testing;

/**
 * Runnable that can throw any throwable.
 */
@FunctionalInterface
public interface ThrowingRunnable {
   void run() throws Throwable;
}
