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
   protected int exitCode;

   public void clear() {
      synchronized (output) {
         output.setLength(0);
      }
   }

   public String output() {
      synchronized (output) {
         // Remove any ANSI
         return output.toString().replaceAll("\\u001B\\[[0-9;]*[a-zA-Z]", "");
      }
   }

   public int exitCode() {
      return exitCode;
   }

   public abstract void send(String data);

   @Override
   public abstract void close();

   public void assertEquals(String expected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output was not equal to expected string after timeout", expected, output()),
            () -> expected.contentEquals(output()), 10_000, 50, TimeUnit.MILLISECONDS);
   }

   public void assertContains(String expected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output did not contain expected string after timeout", expected, output()),
            () -> output().contains(expected), 10_000, 50, TimeUnit.MILLISECONDS);
   }

   public void assertNotContains(String unexpected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output should not contain expected string after timeout", unexpected, output()),
            () -> !output().contains(unexpected), 10_000, 50, TimeUnit.MILLISECONDS);
   }
}
