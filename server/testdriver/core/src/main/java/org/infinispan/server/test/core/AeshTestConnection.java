package org.infinispan.server.test.core;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.aesh.readline.util.Parser;
import org.aesh.terminal.Attributes;
import org.aesh.terminal.BaseDevice;
import org.aesh.terminal.Connection;
import org.aesh.terminal.Device;
import org.aesh.terminal.tty.Capability;
import org.aesh.terminal.tty.Signal;
import org.aesh.terminal.tty.Size;
import org.aesh.terminal.utils.Config;
import org.infinispan.commons.test.Eventually;
import org.junit.ComparisonFailure;

/**
 * @author <a href="mailto:stale.pedersen@jboss.org">Ståle W. Pedersen</a>
 */
public class AeshTestConnection implements Connection, AutoCloseable {

   private Consumer<Size> sizeHandler;
   private Consumer<Signal> signalHandler;
   private Consumer<int[]> stdinHandler;
   private Consumer<int[]> stdOutHandler;
   private Consumer<Void> closeHandler;

   private StringBuilder bufferBuilder;
   private Size size;
   private Attributes attributes;

   private volatile boolean reading = false;

   public AeshTestConnection() {
      this(new Size(80, 20), true);
   }

   public AeshTestConnection(boolean stripAnsiCodes) {
      this(new Size(80, 20), stripAnsiCodes);
   }

   public AeshTestConnection(Size size) {
      this(size, true);
   }

   public AeshTestConnection(Size size, boolean stripAnsiCodes) {
      bufferBuilder = new StringBuilder();
      stdOutHandler = ints -> {
         if (stripAnsiCodes)
            bufferBuilder.append(Parser.stripAwayAnsiCodes(Parser.fromCodePoints(ints)));
         else
            bufferBuilder.append(Parser.fromCodePoints(ints));
      };

      if (size == null)
         this.size = new Size(80, 20);
      else
         this.size = size;

      attributes = new Attributes();
   }

   public void clear() {
      if (bufferBuilder.length() > 0)
         bufferBuilder.delete(0, bufferBuilder.length());
   }

   public String getOutputBuffer() {
      return bufferBuilder.toString();
   }

   @Override
   public Device device() {
      return new BaseDevice() {
         @Override
         public String type() {
            return "vt100";
         }

         @Override
         public boolean getBooleanCapability(Capability capability) {
            return false;
         }

         @Override
         public Integer getNumericCapability(Capability capability) {
            return null;
         }

         @Override
         public String getStringCapability(Capability capability) {
            return null;
         }
      };
   }

   @Override
   public Size size() {
      return size;
   }

   @Override
   public Consumer<Size> getSizeHandler() {
      return sizeHandler;
   }

   @Override
   public void setSizeHandler(Consumer<Size> handler) {
      this.sizeHandler = handler;
   }

   @Override
   public Consumer<Signal> getSignalHandler() {
      return signalHandler;
   }

   @Override
   public void setSignalHandler(Consumer<Signal> handler) {
      signalHandler = handler;
   }

   @Override
   public Consumer<int[]> getStdinHandler() {
      return stdinHandler;
   }

   @Override
   public void setStdinHandler(Consumer<int[]> handler) {
      stdinHandler = handler;
   }

   @Override
   public Consumer<int[]> stdoutHandler() {
      return stdOutHandler;
   }

   @Override
   public void setCloseHandler(Consumer<Void> closeHandler) {
      this.closeHandler = closeHandler;
   }

   @Override
   public Consumer<Void> getCloseHandler() {
      return closeHandler;
   }

   @Override
   public void close() {
      if (reading) { //close() can be invoked multiple times.
         //send a disconnect just in case the connection was left open
         send("disconnect");
      }
      reading = false;
      if (closeHandler != null)
         closeHandler.accept(null);
   }

   public boolean closed() {
      return !reading;
   }

   @Override
   public void openBlocking() {
      //we're not doing anything here, all input will come from the read(..) methods
      reading = true;
   }

   @Override
   public void openNonBlocking() {

   }

   private void doSend(String input) {
      doSend(Parser.toCodePoints(input));
   }

   private void doSend(int[] input) {
      if (reading) {
         if (stdinHandler != null) {
            stdinHandler.accept(input);
         } else {
            try {
               Thread.sleep(10);
               doSend(input);
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
      } else
         throw new RuntimeException("Got input when not reading: " + Arrays.toString(input));
   }

   @Override
   public boolean put(Capability capability, Object... params) {
      return false;
   }

   @Override
   public Attributes getAttributes() {
      return attributes;
   }

   @Override
   public void setAttributes(Attributes attributes) {
      this.attributes = attributes;
   }

   @Override
   public Charset inputEncoding() {
      return Charset.defaultCharset();
   }

   @Override
   public Charset outputEncoding() {
      return Charset.defaultCharset();
   }

   @Override
   public boolean supportsAnsi() {
      return true;
   }

   public void assertEquals(String expected) {
      Eventually.eventually(
            () -> new ComparisonFailure("Expected output was not equal to expected string after timeout", expected, bufferBuilder.toString()),
            () -> expected.equals(bufferBuilder.toString()), 10_000, 50, TimeUnit.MILLISECONDS);
   }

   public void send(String data) {
      doSend(data + Config.getLineSeparator());
   }

   public void assertContains(String expected) {
      Eventually.eventually(
            () -> new ComparisonFailure("Expected output did not contain expected string after timeout", expected, bufferBuilder.toString()),
            () -> bufferBuilder.toString().contains(expected), 10_000, 50, TimeUnit.MILLISECONDS);
   }

   public void assertNotContains(String unexpected) {
      Eventually.eventually(
            () -> new ComparisonFailure("Expected output should not contain expected string after timeout", unexpected, bufferBuilder.toString()),
            () -> !bufferBuilder.toString().contains(unexpected), 10_000, 50, TimeUnit.MILLISECONDS);
   }
}
