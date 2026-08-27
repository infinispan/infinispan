package org.infinispan.cli.test;

import java.util.concurrent.TimeUnit;

import org.infinispan.testing.Eventually;
import org.opentest4j.AssertionFailedError;

/**
 * Abstract base for CLI test terminals. Provides output buffer management
 * and polling-based assertion methods. Subclasses implement the actual
 * I/O mechanism (in-process aesh connection or external process).
 *
 * @since 16.3
 */
public abstract class CliTerminal implements AutoCloseable {

   protected final StringBuilder output = new StringBuilder();

   public void clear() {
      if (!output.isEmpty())
         output.delete(0, output.length());
   }

   public String output() {
      return output.toString();
   }

   public abstract void send(String data);

   @Override
   public abstract void close();

   public void assertEquals(String expected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output was not equal to expected string after timeout", expected, output.toString()),
            () -> expected.contentEquals(output), 10_000, 50, TimeUnit.MILLISECONDS);
   }

   public void assertContains(String expected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output did not contain expected string after timeout", expected, output.toString()),
            () -> output.toString().contains(expected), 10_000, 50, TimeUnit.MILLISECONDS);
   }

   public void assertNotContains(String unexpected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output should not contain expected string after timeout", unexpected, output.toString()),
            () -> !output.toString().contains(unexpected), 10_000, 50, TimeUnit.MILLISECONDS);
   }
}
