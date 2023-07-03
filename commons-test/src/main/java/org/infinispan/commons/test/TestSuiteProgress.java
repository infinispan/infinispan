package org.infinispan.commons.test;

import static org.infinispan.commons.test.Ansi.CYAN;
import static org.infinispan.commons.test.Ansi.GREEN;
import static org.infinispan.commons.test.Ansi.RED;
import static org.infinispan.commons.test.Ansi.RESET;
import static org.infinispan.commons.test.Ansi.YELLOW;

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
   private final AtomicInteger failed = new AtomicInteger(0);
   private final AtomicInteger succeeded = new AtomicInteger(0);
   private final AtomicInteger skipped = new AtomicInteger(0);
   private final PrintStream out;

   public TestSuiteProgress() {
      // Use a system property to avoid color
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
      String reset = "";
      String okColor = "";
      String koColor = "";
      String skipColor = "";
      if (Ansi.useColor && color != null) {
         actualColor = color;
         reset = RESET;
         if (succeeded.get() > 0) {
            okColor = GREEN;
         }
         if (failed.get() > 0) {
            okColor = RED;
         }
         if (skipped.get() > 0) {
            skipColor = CYAN;
         }
      }
      // Must format explicitly, see SUREFIRE-1814
      out.println(String.format("[%sOK: %5s%s, %sKO: %5s%s, %sSKIP: %5s%s] %s%s%s", okColor, succeeded.get(), reset, koColor, failed.get(), reset, skipColor, skipped.get(), reset, actualColor, message, reset));
      if (t != null) {
         t.printStackTrace(out);
      }
   }
}
