package org.infinispan.graalvm.server;

import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.util.Util;
import org.infinispan.server.test.core.CliTerminal;
import org.infinispan.server.test.core.ProcessCliTerminal;
import org.infinispan.server.test.jupiter.InfinispanServerExtension;
import org.infinispan.server.test.jupiter.InfinispanServerExtensionBuilder;
import org.infinispan.testing.Testing;
import org.infinispan.testing.jupiter.tags.Cli;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Integration tests for the native CLI binary against a containerized server.
 *
 * @since 16.3
 */
@Cli
public class NativeCliIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .build();

   private static File workingDir;

   @BeforeAll
   public static void setup() {
      workingDir = new File(Testing.tmpDirectory(NativeCliIT.class));
      Util.recursiveFileRemove(workingDir);
      workingDir.mkdirs();
   }

   @AfterAll
   public static void teardown() {
      Util.recursiveFileRemove(workingDir);
   }

   private static String cliPath() {
      return System.getProperty("infinispan.cli.bin");
   }

   private String hostAddress() {
      return SERVERS.getServerDriver().getServerAddress(0).getHostAddress();
   }

   private String serverUrl() {
      return "http://" + hostAddress() + ":11222";
   }

   private CliTerminal createTerminal(String... cliArgs) {
      return new ProcessCliTerminal(cliPath(), workingDir.getAbsolutePath(), cliArgs);
   }

   private CliResult runBatch(String... commands) throws Exception {
      Path batchFile = Files.createTempFile(workingDir.toPath(), "batch", ".cli");
      Files.write(batchFile, Arrays.asList(commands), StandardCharsets.UTF_8);
      return run("-c", serverUrl(), "-f", batchFile.toString());
   }

   private CliResult run(String... args) throws Exception {
      List<String> cmd = new ArrayList<>();
      cmd.add(cliPath());
      cmd.addAll(Arrays.asList(args));
      ProcessBuilder pb = new ProcessBuilder(cmd);
      pb.environment().put("ISPN_CLI_DIR", workingDir.getAbsolutePath());
      pb.redirectErrorStream(true);
      Process p = pb.start();
      StringBuilder sb = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
         String line;
         while ((line = reader.readLine()) != null) {
            sb.append(line).append(System.lineSeparator());
         }
      }
      assertTrue(p.waitFor(30, TimeUnit.SECONDS), "CLI process timed out");
      return new CliResult(p.exitValue(), sb.toString());
   }

   @Test
   public void testCliBatch() throws Exception {
      CliResult result = runBatch(
            "create cache --template=org.infinispan.DIST_SYNC mybatch",
            "cd caches/mybatch",
            "put k1 v1",
            "get k1"
      );
      assertEquals(0, result.exitCode, "batch failed: " + result.output);
      assertTrue(result.output.contains("v1"), "Expected v1 in output: " + result.output);

      RestClient client = SERVERS.rest().create();
      RestResponse restResponse = sync(client.cache("mybatch").exists());
      assertEquals(204, restResponse.status());
   }

   @Test
   public void testCliCacheOperations() throws Exception {
      runBatch("create cache --template=org.infinispan.DIST_SYNC clitest");
      CliResult put = runBatch("put --cache=clitest k1 v1");
      assertEquals(0, put.exitCode, "put failed: " + put.output);

      CliResult get = runBatch("get --cache=clitest k1");
      assertEquals(0, get.exitCode, "get failed: " + get.output);
      assertTrue(get.output.contains("v1"), "Expected v1 in output: " + get.output);

      CliResult ls = runBatch("ls caches/clitest");
      assertEquals(0, ls.exitCode, "ls failed: " + ls.output);
      assertTrue(ls.output.contains("k1"), "Expected k1 in output: " + ls.output);
   }

   @Test
   public void testCliCredentials() {
      try (CliTerminal terminal = createTerminal()) {
         String keyStore = Paths.get(System.getProperty("build.directory", ""), "key.store").toAbsolutePath().toString();
         terminal.send("credentials add --path=" + keyStore + " --password=secret --credential=credential password");
         terminal.send("credentials add --path=" + keyStore + " --password=secret --credential=credential another");
         terminal.clear();
         terminal.send("credentials ls --path=" + keyStore + " --password=secret");
         terminal.assertContains("password");
         terminal.assertContains("another");
      }
   }

   @Test
   public void testCliConfigPersistence() {
      try (CliTerminal terminal = createTerminal()) {
         terminal.send("config set autoconnect-url " + serverUrl());
         terminal.clear();
         terminal.send("config get autoconnect-url");
         terminal.assertContains(serverUrl());
         terminal.send("config set autoconnect-url");
      }
   }

   @Test
   public void testCliServerReport() throws Exception {
      CliResult result = runBatch("server report");
      assertEquals(0, result.exitCode, "server report failed: " + result.output);
      assertTrue(result.output.contains("tar.gz"), "Expected tar.gz path in output: " + result.output);
   }

   record CliResult(int exitCode, String output) {}
}
