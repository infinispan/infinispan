package org.infinispan.cli.test;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

import org.aesh.command.shell.Shell;
import org.aesh.readline.prompt.Prompt;
import org.aesh.terminal.Key;
import org.aesh.terminal.tty.Size;
import org.aesh.terminal.utils.Config;
import org.aesh.terminal.utils.Parser;
import org.infinispan.testing.Eventually;
import org.opentest4j.AssertionFailedError;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 15.0
 **/
public class AeshTestShell implements Shell {
   private final StringBuilder output = new StringBuilder();
   private final Deque<String> readLineResponses;

   public AeshTestShell() {
      this.readLineResponses = new ArrayDeque<>();

   }

   public AeshTestShell(Collection<String> lines) {
      this.readLineResponses = new ArrayDeque<>(lines);
   }


   @Override
   public void write(String msg, boolean paging) {
      output.append(msg);
   }

   @Override
   public void writeln(String msg, boolean paging) {
      output.append(msg).append(Config.getLineSeparator());
   }

   @Override
   public void write(int[] out) {
      output.append(Parser.fromCodePoints(out));
   }

   @Override
   public void write(char out) {
      output.append(out);
   }

   @Override
   public String readLine() {
      return readLineResponses.removeFirst();
   }

   @Override
   public String readLine(Prompt prompt) {
      return readLine();
   }

   @Override
   public Key read() throws InterruptedException {
      return null;
   }

   @Override
   public Key read(long timeout, TimeUnit unit) throws InterruptedException {
      return null;
   }

   @Override
   public Key read(Prompt prompt) throws InterruptedException {
      return null;
   }

   @Override
   public boolean enableAlternateBuffer() {
      return false;
   }

   @Override
   public boolean enableMainBuffer() {
      return false;
   }

   @Override
   public Size size() {
      return null;
   }

   @Override
   public void clear() {
      output.setLength(0);
   }

   public String getBuffer() {
      return output.toString();
   }

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
