package org.infinispan.server.test.core;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.function.Consumer;

import org.aesh.terminal.Attributes;
import org.aesh.terminal.BaseDevice;
import org.aesh.terminal.Connection;
import org.aesh.terminal.Device;
import org.aesh.terminal.tty.Capability;
import org.aesh.terminal.tty.Signal;
import org.aesh.terminal.tty.Size;
import org.aesh.terminal.utils.Config;
import org.aesh.terminal.utils.Parser;

/**
 * @author <a href="mailto:stale.pedersen@jboss.org">Ståle W. Pedersen</a>
 */
public class AeshTestConnection extends CliTerminal implements Connection {

   private Consumer<Size> sizeHandler;
   private Consumer<Signal> signalHandler;
   private Consumer<int[]> stdinHandler;
   private final Consumer<int[]> stdOutHandler;
   private Consumer<Void> closeHandler;

   private final Size size;
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

   @Override
   public void send(String data) {
      doSend(data + Config.getLineSeparator());
   }
}
