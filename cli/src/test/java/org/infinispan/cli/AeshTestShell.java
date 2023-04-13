package org.infinispan.cli;

import java.util.concurrent.TimeUnit;

import org.aesh.command.shell.Shell;
import org.aesh.readline.Prompt;
import org.aesh.readline.terminal.Key;
import org.aesh.readline.util.Parser;
import org.aesh.terminal.tty.Size;
import org.aesh.terminal.utils.Config;
import org.infinispan.commons.test.Eventually;
import org.junit.ComparisonFailure;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 15.0
 **/
public class AeshTestShell implements Shell {
   private final StringBuilder bufferBuilder = new StringBuilder();


   @Override
   public void write(String msg, boolean paging) {
      bufferBuilder.append(msg);
   }

   @Override
   public void writeln(String msg, boolean paging) {
      bufferBuilder.append(msg).append(Config.getLineSeparator());
   }

   @Override
   public void write(int[] out) {
      bufferBuilder.append(Parser.fromCodePoints(out));
   }

   @Override
   public void write(char out) {
      bufferBuilder.append(out);
   }

   @Override
   public String readLine() {
      return null;
   }

   @Override
   public String readLine(Prompt prompt) {
      return null;
   }

   @Override
   public Key read() throws InterruptedException {
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
      bufferBuilder.setLength(0);
   }

   public String getBuffer() {
      return bufferBuilder.toString();
   }

   public void assertEquals(String expected) {
      Eventually.eventually(
            () -> new ComparisonFailure("Expected output was not equal to expected string after timeout", expected, bufferBuilder.toString()),
            () -> expected.contentEquals(bufferBuilder), 10_000, 50, TimeUnit.MILLISECONDS);
   }

   public void assertContains(String expected) {
      Eventually.eventually(
            () -> new ComparisonFailure("Expected output did not contain expected string after timeout", expected, bufferBuilder.toString()),
            () -> bufferBuilder.toString().contains(expected), 10_000, 50, TimeUnit.MILLISECONDS);
   }
}
