package org.infinispan.commons.test;

import java.io.PrintStream;
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
   private static final String RED = "\u001b[31m";
   private static final String GREEN = "\u001b[32m";
   private static final String YELLOW = "\u001b[33m";
   private static final String RESET = "\u001b[0m";

   private AtomicInteger failed = new AtomicInteger(0);
   private AtomicInteger succeeded = new AtomicInteger(0);
   private AtomicInteger skipped = new AtomicInteger(0);
   private final PrintStream out;
   private final boolean useColor;

   public TestSuiteProgress() {
      // Use a system property to avoid color
      useColor = !Boolean.getBoolean("ansi.strip");
      out = System.out;
   }

   void testStarted(String name) {
      String message = "Test starting: " + name;
      progress(message);
      log.info(message);
   }

   void testSucceeded(String name) {
      succeeded.incrementAndGet();
      String message = "Test succeeded: " + name;
      progress(GREEN, message);
      log.info(message);

   }

   void testFailed(String name, Throwable exception) {
      failed.incrementAndGet();
      String message = "Test failed: " + name;
      progress(RED, message, exception);
      log.error(message, exception);
   }

   void testIgnored(String name) {
      skipped.incrementAndGet();
      String message = "Test ignored: " + name;
      progress(YELLOW, message);
      log.info(message);
   }

   void testAssumptionFailed(String name, Throwable exception) {
      skipped.incrementAndGet();
      String message = "Test assumption failed: " + name;
      progress(YELLOW, message, exception);
      log.info(message, exception);
   }

   void configurationStarted(String name) {
      log.debug("Test configuration started: " + name);
   }

   void configurationFinished(String name) {
      log.debug("Test configuration finished: " + name);
   }

   void configurationFailed(String name, Throwable exception) {
      failed.incrementAndGet();
      String message = "Test configuration failed: " + name;
      progress(RED, message, exception);
      log.error(message, exception);
   }

   /**
    * Write a fake test failures in the test-failures log, for {@code process_trace_logs.sh}
    */
   public static void fakeTestFailure(String name, Throwable exception) {
      String message = "Test failed: " + name;
      System.out.printf("[TestSuiteProgress] %s%n", message);
      log.error(message, exception);
   }

   public static void printTestJDKInformation() {
      String message = "Running tests with JDK " + System.getProperty("java.version");
      System.out.println(message);
      log.info(message);
   }

   private void progress(CharSequence message) {
      progress(null, message, null);
   }

   private void progress(String color, CharSequence message) {
      progress(color, message, null);
   }

   private synchronized void progress(String color, CharSequence message, Throwable t) {
      String actualColor = "";
      String actualReset = "";
      if (useColor && color != null) {
         actualColor = color;
         actualReset = RESET;
      }
      // Must format explicitly, see SUREFIRE-1814
      out.println(String.format("%s[OK: %5s, KO: %5s, SKIP: %5s] %s%s", actualColor, succeeded.get(), failed.get(), skipped.get(), message, actualReset));
      if (t != null) {
         t.printStackTrace(out);
      }
   }
}
