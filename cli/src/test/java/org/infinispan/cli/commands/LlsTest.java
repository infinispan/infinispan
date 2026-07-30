package org.infinispan.cli.commands;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.cli.impl.StreamShell;
import org.infinispan.commons.util.Util;
import org.infinispan.testing.Eventually;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

/**
 * @since 16.3
 **/
public class LlsTest {

   @Test
   public void testLls() throws IOException, InterruptedException {
      File workingDir = new File(org.infinispan.testing.Testing.tmpDirectory(LlsTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Files.writeString(Path.of(workingDir.toString(), "alpha.txt"), "alpha");
      Files.createDirectories(Path.of(workingDir.toString(), "beta"));
      Files.writeString(Path.of(workingDir.toString(), "zeta.txt"), "zeta");

      String output = runLls(workingDir, "lls");
      assertTrue(output.contains("alpha.txt"), output);
      assertTrue(output.contains("beta/"), output);
      assertTrue(output.contains("zeta.txt"), output);
   }

   @Test
   public void testLlsPath() throws IOException, InterruptedException {
      File workingDir = new File(org.infinispan.testing.Testing.tmpDirectory(LlsTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Files.createDirectories(Path.of(workingDir.toString(), "beta"));
      Files.writeString(Path.of(workingDir.toString(), "beta", "inner.txt"), "inner");

      String output = runLls(workingDir, "lls beta");
      assertTrue(output.contains("inner.txt"), output);
   }

   private String runLls(File workingDir, String command) throws IOException, InterruptedException {
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());
      properties.put("user.dir", workingDir.getAbsolutePath());

      PipedOutputStream pipe = new PipedOutputStream();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      StreamShell shell = new StreamShell(new PipedInputStream(pipe), new PrintStream(out));
      Thread thread = new Thread(() -> CLI.main(shell, properties, "-f", "-"));
      thread.start();
      try (PrintWriter pw = new PrintWriter(pipe, true)) {
         pw.println(command);
         Eventually.eventually(
               () -> new AssertionFailedError("Expected lls output after timeout", command, out.toString(StandardCharsets.UTF_8)),
               () -> out.toString(StandardCharsets.UTF_8).contains("txt"), 10_000, 50, TimeUnit.MILLISECONDS);
      }
      thread.join();
      return out.toString(StandardCharsets.UTF_8);
   }
}
