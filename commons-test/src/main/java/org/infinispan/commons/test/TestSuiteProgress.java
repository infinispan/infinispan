package org.infinispan.commons.test;


import static org.fusesource.jansi.Ansi.ansi;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.fusesource.jansi.Ansi;
import org.jboss.logging.Logger;


/**
 * Helper class for test listeners.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class TestSuiteProgress {

   private static final Logger log = Logger.getLogger(TestSuiteProgress.class);

   private AtomicInteger failed = new AtomicInteger(0);
   private AtomicInteger succeeded = new AtomicInteger(0);
   private AtomicInteger skipped = new AtomicInteger(0);
   private final PrintStream out;
   private final boolean useColor;
   private final String prefix;;

   public TestSuiteProgress() {
      // Use a system property to avoid color
      useColor = !Boolean.getBoolean("jansi.strip");
      // Prefix console lines only when our output has been captured (e.g. by surefire's fork runner)
      prefix = System.out.getClass().equals(PrintStream.class) ? "" : "H  ,";
      // Grab the low-level output descriptor as System.out might already have been redirected
      out = new PrintStream(new FileOutputStream(FileDescriptor.out));
   }

   void testStarted(String name) {
      String message = "Test starting: " + name;
      consoleLog(message);
      log.info(message);
   }

   void testFinished(String name) {
      String message = "Test succeeded: " + name;
      consoleLog(message, Ansi.Color.GREEN);
      log.info(message);
      succeeded.incrementAndGet();
      printStatus();
   }

   void testFailed(String name, Throwable exception) {
      String message = "Test failed: " + name;
      consoleLog(message, Ansi.Color.RED);
      log.error(message, exception);
      failed.incrementAndGet();
      printStatus();
   }

   void testIgnored(String name) {
      String message = "Test ignored: " + name;
      consoleLog(message, Ansi.Color.CYAN);
      log.info(message);
      skipped.incrementAndGet();
      printStatus();
   }

   void testAssumptionFailed(String name, Throwable exception) {
      String message = "Test assumption failed: " + name;
      consoleLog(message, Ansi.Color.RED);
      log.info(message, exception);
      skipped.incrementAndGet();
      printStatus();
   }

   void setupFailed(String name, Throwable exception) {
      String message = "Test setup failed: " + name;
      consoleLog(message, Ansi.Color.RED);
      log.error(message, exception);
      failed.incrementAndGet();
      printStatus();
   }

   void printStatus() {
      String message = "Tests succeeded: " + succeeded.get() + ", failed: " + failed.get() + ", skipped: " +
            skipped.get();
      consoleLog(message);
   }

   void consoleLog(String message) {
      out.printf("%s%s\n", prefix, message);
   }

   void consoleLog(String message, Ansi.Color color) {
      if (useColor)
         out.print(prefix + ansi().fg(color).a(message).reset().newline());
      else
         consoleLog(message);
   }
}
