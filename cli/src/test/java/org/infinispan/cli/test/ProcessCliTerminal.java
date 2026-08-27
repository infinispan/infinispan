package org.infinispan.cli.test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.infinispan.commons.util.Util;

/**
 * {@link CliTerminal} that launches the CLI binary as an external process.
 * Commands are written to process stdin; output is read from stdout into the
 * shared buffer by a daemon reader thread.
 *
 * @since 16.3
 */
public class ProcessCliTerminal extends CliTerminal {

   private final Process process;
   private final BufferedWriter stdin;
   private final ExecutorService outputReaderExecutor;

   public ProcessCliTerminal(String cliPath, String workingDir, String... cliArgs) {
      List<String> cmd = new ArrayList<>();
      cmd.add(cliPath);
      cmd.addAll(Arrays.asList(cliArgs));
      ProcessBuilder pb = new ProcessBuilder(cmd);
      pb.redirectErrorStream(true).environment().put("ISPN_CLI_DIR", workingDir);
      try {
         process = pb.start();
      } catch (IOException e) {
         throw new RuntimeException("Failed to start CLI process: " + cliPath, e);
      }
      stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
      // Start a dedicated daemon thread to continuously consume the process output
      outputReaderExecutor = Executors.newSingleThreadExecutor(r -> {
         Thread t = new Thread(r, "ProcessCliTerminal-Output-Reader");
         t.setDaemon(true); // Don't block JVM shutdown
         return t;
      });

      outputReaderExecutor.submit(this::readOutput);
   }

   private void readOutput() {
      try (InputStreamReader reader = new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8)) {
         char[] buffer = new char[1024];
         int charsRead;
         while ((charsRead = reader.read(buffer)) != -1) {
            synchronized (output) {
               output.append(buffer, 0, charsRead);
            }
         }
      } catch (IOException e) {
         // Process killed or stream closed
      }
   }

   @Override
   public void send(String data) {
      try {
         stdin.write(data);
         stdin.write(System.lineSeparator());
         stdin.flush();
      } catch (IOException e) {
         throw new RuntimeException("Failed to send command to CLI process", e);
      }
   }

   @Override
   public void close() {
      Util.close(stdin);
      if (outputReaderExecutor != null) {
         outputReaderExecutor.shutdownNow();
      }
      if (process != null && process.isAlive()) {
         process.destroy();
      }
   }
}
