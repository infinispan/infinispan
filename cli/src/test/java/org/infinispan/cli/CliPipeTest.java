package org.infinispan.cli;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.StreamShell;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Eventually;
import org.infinispan.commons.util.Util;
import org.junit.ComparisonFailure;
import org.junit.Test;

/**
 * @since 14.0
 **/
public class CliPipeTest {
   @Test
   public void testCliBatchPipe() throws IOException, InterruptedException {
      File workingDir = new File(CommonsTestingUtil.tmpDirectory(CliPipeTest.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
      Properties properties = new Properties(System.getProperties());
      properties.put("cli.dir", workingDir.getAbsolutePath());

      PipedOutputStream pipe = new PipedOutputStream();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      StreamShell shell = new StreamShell(new PipedInputStream(pipe), new PrintStream(out));
      Thread thread = new Thread(() -> CLI.main(shell, new String[]{"-f", "-"}, properties));
      thread.start();
      PrintWriter pw = new PrintWriter(pipe, true);
      pw.println("echo Piped");
      Eventually.eventually(
            () -> new ComparisonFailure("Expected output was not equal to expected string after timeout", "Piped", out.toString(StandardCharsets.UTF_8)),
            () -> out.toString(StandardCharsets.UTF_8).startsWith("Piped"), 10_000, 50, TimeUnit.MILLISECONDS);
      pw.close();
      thread.join();
   }
}
