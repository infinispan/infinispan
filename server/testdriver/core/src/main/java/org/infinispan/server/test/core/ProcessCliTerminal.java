package org.infinispan.server.test.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link CliTerminal} that launches the CLI binary as an external process.
 * Commands are written to process stdin; output is read from stdout into the
 * shared buffer by a daemon reader thread.
 *
 * @since 16.3
 */
public class ProcessCliTerminal extends CliTerminal {

   private final Process process;
   private final OutputStream stdin;
   private final Thread readerThread;

   public ProcessCliTerminal(String cliPath, String workingDir, String... cliArgs) {
      List<String> cmd = new ArrayList<>();
      cmd.add(cliPath);
      cmd.addAll(Arrays.asList(cliArgs));
      ProcessBuilder pb = new ProcessBuilder(cmd);
      pb.environment().put("ISPN_CLI_DIR", workingDir);
      pb.redirectErrorStream(true);
      try {
         process = pb.start();
      } catch (IOException e) {
         throw new RuntimeException("Failed to start CLI process: " + cliPath, e);
      }
      stdin = process.getOutputStream();
      readerThread = new Thread(() -> {
         try (BufferedReader reader = new BufferedReader(
               new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
               synchronized (bufferBuilder) {
                  bufferBuilder.append(line).append(System.lineSeparator());
               }
            }
         } catch (IOException e) {
            // process closed
         }
      });
      readerThread.setDaemon(true);
      readerThread.setName("ProcessCliTerminal-reader");
      readerThread.start();
   }

   @Override
   public void send(String data) {
      try {
         stdin.write((data + "\n").getBytes(StandardCharsets.UTF_8));
         stdin.flush();
      } catch (IOException e) {
         throw new RuntimeException("Failed to send command to CLI process", e);
      }
   }

   @Override
   public String getOutputBuffer() {
      synchronized (bufferBuilder) {
         return bufferBuilder.toString();
      }
   }

   @Override
   public void clear() {
      synchronized (bufferBuilder) {
         if (!bufferBuilder.isEmpty())
            bufferBuilder.delete(0, bufferBuilder.length());
      }
   }

   @Override
   public void close() {
      try {
         stdin.write("quit\n".getBytes(StandardCharsets.UTF_8));
         stdin.flush();
      } catch (IOException e) {
         // stdin may already be closed
      }
      try {
         if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
         }
      } catch (InterruptedException e) {
         process.destroyForcibly();
         Thread.currentThread().interrupt();
      }
   }
}
