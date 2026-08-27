package org.infinispan.cli.test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.StreamShell;

/**
 * {@link CliTerminal} that runs the CLI embedded in a background thread.
 * Commands are written to a pipe connected to the CLI's stdin; output is
 * captured to the shared buffer via a capturing {@link PrintStream}.
 *
 * @since 16.3
 */
public class EmbeddedCliTerminal extends CliTerminal {

   private final PipedOutputStream stdinPipe;
   private final Thread cliThread;

   public EmbeddedCliTerminal(Properties cliProperties) {
      try {
         stdinPipe = new PipedOutputStream();
         PipedInputStream stdinIn = new PipedInputStream(stdinPipe);
         PrintStream captureOut = new PrintStream(new CapturingOutputStream(), true, StandardCharsets.UTF_8);
         StreamShell shell = new StreamShell(stdinIn, captureOut);
         cliThread = new Thread(() -> exitCode = CLI.main(shell, cliProperties, "--on-error=IGNORE", "-f", "-"));
         cliThread.setDaemon(true);
         cliThread.setName("EmbeddedCliTerminal");
         cliThread.start();
      } catch (IOException e) {
         throw new UncheckedIOException(e);
      }
   }

   @Override
   public void send(String data) {
      try {
         stdinPipe.write((data + "\n").getBytes(StandardCharsets.UTF_8));
         stdinPipe.flush();
      } catch (IOException e) {
         throw new UncheckedIOException(e);
      }
   }

   @Override
   public void close() {
      try {
         stdinPipe.close();
      } catch (IOException e) {
         // pipe may already be closed
      }
      try {
         cliThread.join(30_000);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
   }

   private class CapturingOutputStream extends OutputStream {
      @Override
      public void write(int b) {
         synchronized (output) {
            output.append((char) b);
         }
      }

      @Override
      public void write(byte[] b, int off, int len) {
         synchronized (output) {
            output.append(new String(b, off, len, StandardCharsets.UTF_8));
         }
      }
   }
}
