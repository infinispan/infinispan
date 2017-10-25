package org.infinispan.commons.test;


import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.logging.Logger;


/**
 * Helper class for test listeners.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class TestSuiteProgress {

   private static final Logger log = Logger.getLogger(TestSuiteProgress.class);

   private static AtomicInteger failed = new AtomicInteger(0);
   private static AtomicInteger succeeded = new AtomicInteger(0);
   private static AtomicInteger skipped = new AtomicInteger(0);

   static void testStarted(String name) {
      String message = "Test starting: " + name;
      consoleLog(message);
      log.info(message);
   }

   static void testFinished(String name) {
      String message = "Test succeeded: " + name;
      consoleLog(message);
      log.info(message);
      succeeded.incrementAndGet();
      printStatus();
   }

   static void testFailed(String name, Throwable exception) {
      String message = "Test failed: " + name;
      consoleLog(message);
      log.error(message, exception);
      failed.incrementAndGet();
      printStatus();
   }

   static void testIgnored(String name) {
      String message = "Test ignored: " + name;
      consoleLog(message);
      log.info(message);
      skipped.incrementAndGet();
      printStatus();
   }

   static void testAssumptionFailed(String name, Throwable exception) {
      String message = "Test assumption failed: " + name;
      consoleLog(message);
      log.info(message, exception);
      skipped.incrementAndGet();
      printStatus();
   }

   static void setupFailed(String name, Throwable exception) {
      String message = "Test setup failed: " + name;
      consoleLog(message);
      log.error(message, exception);
      failed.incrementAndGet();
      printStatus();
   }

   private static void printStatus() {
      String message = "Tests succeeded: " + succeeded.get() + ", failed: " + failed.get() + ", skipped: " +
            skipped.get();
      consoleLog(message);
   }

   private static void consoleLog(String message) {
      System.out.println("[" + TestSuiteProgress.class.getSimpleName() + "] " + message);
   }
}
