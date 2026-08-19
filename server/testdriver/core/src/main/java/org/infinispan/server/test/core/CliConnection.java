package org.infinispan.server.test.core;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.aesh.terminal.Attributes;
import org.aesh.terminal.BaseDevice;
import org.aesh.terminal.Connection;
import org.aesh.terminal.Device;
import org.aesh.terminal.Key;
import org.aesh.terminal.tty.Capability;
import org.aesh.terminal.tty.Signal;
import org.aesh.terminal.tty.Size;
import org.aesh.terminal.utils.Config;
import org.aesh.terminal.utils.Parser;
import org.infinispan.testing.Eventually;
import org.opentest4j.AssertionFailedError;

/**
 * @author <a href="mailto:stale.pedersen@jboss.org">Ståle W. Pedersen</a>
 */
public class CliConnection implements Connection, AutoCloseable {

   public static final int READ_SLEEP = 10;
   private Consumer<Size> sizeHandler;
   private Consumer<Signal> signalHandler;
   private Consumer<int[]> stdinHandler;
   private final Consumer<int[]> stdOutHandler;
   private Consumer<Void> closeHandler;

   private final StringBuilder bufferBuilder;
   private final Size size;
   private Attributes attributes;

   private volatile boolean reading = false;
   // Track stdinHandler changes to detect readline cycle transitions.
   // When the handler changes, doRead() briefly yields to let the
   // processing thread drain any buffered input.
   private volatile boolean handlerChanged = false;

   public CliConnection() {
      this(new Size(80, 20), true);
   }

   public CliConnection(boolean stripAnsiCodes) {
      this(new Size(80, 20), stripAnsiCodes);
   }

   public CliConnection(Size size) {
      this(size, true);
   }

   public CliConnection(Size size, boolean stripAnsiCodes) {
      bufferBuilder = new StringBuilder();
      stdOutHandler = ints -> {
         if (stripAnsiCodes) bufferBuilder.append(Parser.stripAwayAnsiCodes(Parser.fromCodePoints(ints)));
         else bufferBuilder.append(Parser.fromCodePoints(ints));
      };

      if (size == null) this.size = new Size(80, 20);
      else this.size = size;

      attributes = new Attributes();
   }

   public void clear() {
      if (!bufferBuilder.isEmpty()) bufferBuilder.delete(0, bufferBuilder.length());
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
   public Consumer<Size> sizeHandler() {
      return sizeHandler;
   }

   @Override
   public void setSizeHandler(Consumer<Size> handler) {
      this.sizeHandler = handler;
   }

   @Override
   public Consumer<Signal> signalHandler() {
      return signalHandler;
   }

   @Override
   public void setSignalHandler(Consumer<Signal> handler) {
      signalHandler = handler;
   }

   @Override
   public Consumer<int[]> stdinHandler() {
      return stdinHandler;
   }

   @Override
   public void setStdinHandler(Consumer<int[]> handler) {
      stdinHandler = handler;
      handlerChanged = true;
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
   public Consumer<Void> closeHandler() {
      return closeHandler;
   }

   @Override
   public void close() {
      reading = false;
      if (closeHandler != null) closeHandler.accept(null);
   }

   @Override
   public void openBlocking() {
      //we're not doing anything here, all input will come from the read(..) methods
      reading = true;
   }

   @Override
   public void openNonBlocking() {

   }

   private void doRead(int[] input) {
      if (reading) {
         // If the stdinHandler was recently changed (e.g., readline cycle
         // transition), yield briefly to let the processing thread drain
         // any buffered input and start the next readline cycle.
         if (handlerChanged) {
            handlerChanged = false;
            try {
               Thread.sleep(READ_SLEEP);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         }
         if (stdinHandler != null) {
            stdinHandler.accept(input);
         } else {
            try {
               Thread.sleep(READ_SLEEP);
               doRead(input);
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
      } else throw new RuntimeException("Got input when not reading: " + Arrays.toString(input));
   }

   @Override
   public boolean put(Capability capability, Object... params) {
      return false;
   }

   @Override
   public Attributes attributes() {
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

   public void read(int... data) {
      doRead(data);
   }

   public void read(Key key) {
      doRead(key.getKeyValues());
   }

   public void read(String data) {
      doRead(Parser.toCodePoints(data));
   }

   public void assertEquals(String expected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output was not equal to expected string after timeout", expected, bufferBuilder.toString()),
            () -> expected.contentEquals(bufferBuilder),
            10_000, 50, TimeUnit.MILLISECONDS);
   }

   public void send(String data) {
      read(data + Config.getLineSeparator());
   }

   public void assertContains(String expected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output did not contain expected string after timeout", expected, bufferBuilder.toString()),
            () -> bufferBuilder.toString().contains(expected),
            10_000, 50, TimeUnit.MILLISECONDS);
   }

   public void assertNotContains(String unexpected) {
      Eventually.eventually(
            () -> new AssertionFailedError("Expected output should not contain expected string after timeout", unexpected, bufferBuilder.toString()),
            () -> !bufferBuilder.toString().contains(unexpected),
            10_000, 50, TimeUnit.MILLISECONDS);
   }
}
