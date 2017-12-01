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

   void testFinished(String name) {
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

   void setupFailed(String name, Throwable exception) {
      failed.incrementAndGet();
      String message = "Test setup failed: " + name;
      progress(RED, message);
      log.error(message, exception);
   }

   void progress(CharSequence message) {
      progress(null, message, null);
   }

   void progress(String color, CharSequence message) {
      progress(color, message, null);
   }

   synchronized void progress(String color, CharSequence message, Throwable t) {
      if (useColor && color != null) {
         out.print(color);
      }
      out.printf("[OK: %5s, KO: %5s, SKIP: %5s] %s%n", succeeded.get(), failed.get(), skipped.get(), message);
      if (t != null) {
         t.printStackTrace(out);
      }
      if (useColor && color != null) {
         out.print(RESET);
      }
   }
}
