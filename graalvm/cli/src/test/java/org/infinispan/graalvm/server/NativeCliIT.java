package org.infinispan.graalvm.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;

import org.infinispan.cli.test.CliExtension;
import org.infinispan.cli.test.CliTerminal;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.util.Util;
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

   @RegisterExtension
   CliExtension cli = new CliExtension();

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

   @Test
   public void testCliBatch() {
      CliTerminal terminal = cli.batch(
            "connect " + serverUrl(),
            "create cache --template=org.infinispan.DIST_SYNC mybatch",
            "cd caches/mybatch",
            "put k1 v1",
            "get k1"
      );
      assertEquals(0, terminal.exitCode(), "batch failed: " + terminal.output());
      assertThat(terminal.output()).contains("v1");

      RestClient client = SERVERS.rest().create();
      RestResponse restResponse = sync(client.cache("mybatch").exists());
      assertEquals(204, restResponse.status());
   }

   @Test
   public void testCliCacheOperations() {
      try (CliTerminal terminal = cli.interactive()) {
         terminal.send("connect " + serverUrl());
         terminal.send("create cache --template=org.infinispan.DIST_SYNC clitest");
         terminal.send("put --cache=clitest k1 v1");
         terminal.clear();
         terminal.send("get --cache=clitest k1");
         terminal.assertContains("v1");
         terminal.clear();
         terminal.send("ls caches/clitest");
         terminal.assertContains("k1");
      }
   }

   @Test
   public void testCliCredentials() {
      try (CliTerminal terminal = cli.interactive()) {
         String keyStore = workingDir.toPath().resolve("key.store").toAbsolutePath().toString();
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
      try (CliTerminal terminal = cli.interactive()) {
         terminal.send("config set autoconnect-url " + serverUrl());
         terminal.clear();
         terminal.send("config get autoconnect-url");
         terminal.assertContains(serverUrl());
         terminal.send("config set autoconnect-url");
      }
   }

   @Test
   public void testCliServerReport() {
      try (CliTerminal terminal = cli.batch("connect " + serverUrl(), "lcd " + workingDir.getAbsolutePath(), "server report")) {
         assertEquals(0, terminal.exitCode(), "serverReport failed: " + terminal.output());
         assertThat(terminal.output()).contains("tar.gz");
      }
   }
}
