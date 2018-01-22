package org.infinispan.test;

/**
 * Runnable that can throw any exception.
 *
 * @author Dan Berindei
 * @since 9.2
 */
public interface ExceptionRunnable {
   void run() throws Exception;
}
