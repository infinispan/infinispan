package org.infinispan.server.test.core;

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

   protected final StringBuilder bufferBuilder = new StringBuilder();

   public void clear() {
      if (!bufferBuilder.isEmpty())
         bufferBuilder.delete(0, bufferBuilder.length());
   }

   public String getOutputBuffer() {
      return bufferBuilder.toString();
   }

   public abstract void send(String data);

   @Override
   public abstract void close();

   public void assertEquals(String expected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output was not equal to expected string after timeout", expected, bufferBuilder.toString()),
            () -> expected.contentEquals(bufferBuilder), 10_000, 50, TimeUnit.MILLISECONDS);
   }

   public void assertContains(String expected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output did not contain expected string after timeout", expected, bufferBuilder.toString()),
            () -> bufferBuilder.toString().contains(expected), 10_000, 50, TimeUnit.MILLISECONDS);
   }

   public void assertNotContains(String unexpected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output should not contain expected string after timeout", unexpected, bufferBuilder.toString()),
            () -> !bufferBuilder.toString().contains(unexpected), 10_000, 50, TimeUnit.MILLISECONDS);
   }
}
