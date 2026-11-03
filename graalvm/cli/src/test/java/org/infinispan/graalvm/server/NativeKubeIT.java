package org.infinispan.graalvm.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.Util;
import org.infinispan.testing.Testing;
import org.infinispan.testing.jupiter.tags.Cli;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the native CLI binary works when it is installed as a <code>kubectl</code> plugin, i.e. when it is
 * invoked as <code>kubectl-infinispan</code>. The Kubernetes client resolves its implementation by name and maps the
 * Kubernetes model with Jackson, neither of which the native image can discover on its own, so these commands only
 * work if the reachability metadata produced by
 * {@link org.infinispan.cli.graalvm.NativeMetadataProvider} is complete.
 * <p>
 * The tests run against a stub API server rather than a real cluster.
 *
 * @since 16.3
 */
@Cli
public class NativeKubeIT {

   private static final String NAMESPACE = "default";

   private static FakeKubeApiServer server;
   private static Path workingDir;
   private static Path kubeConfig;
   private static Path plugin;

   @BeforeAll
   public static void setup() throws IOException {
      assumeTrue(System.getProperty("infinispan.cli.bin") != null, "Requires the native CLI binary");

      workingDir = Path.of(Testing.tmpDirectory(NativeKubeIT.class));
      Util.recursiveFileRemove(workingDir.toFile());
      Files.createDirectories(workingDir);

      server = FakeKubeApiServer.start(NAMESPACE);

      kubeConfig = workingDir.resolve("kubeconfig");
      Files.writeString(kubeConfig, server.kubeConfig());

      // The CLI only enables the Kubernetes commands when it is invoked as a kubectl plugin
      plugin = workingDir.resolve("kubectl-infinispan");
      Path cli = Path.of(System.getProperty("infinispan.cli.bin"));
      try {
         Files.createLink(plugin, cli);
      } catch (IOException | UnsupportedOperationException e) {
         Files.copy(cli, plugin);
      }
      plugin.toFile().setExecutable(true);
   }

   @AfterAll
   public static void teardown() {
      if (server != null) {
         server.close();
      }
      if (workingDir != null) {
         Util.recursiveFileRemove(workingDir.toFile());
      }
   }

   private static String run(int expectedExitCode, String... args) {
      List<String> command = new ArrayList<>();
      command.add(plugin.toString());
      command.addAll(Arrays.asList(args));
      ProcessBuilder pb = new ProcessBuilder(command);
      pb.environment().put("KUBECONFIG", kubeConfig.toString());
      pb.redirectErrorStream(true);
      try {
         Process process = pb.start();
         String output;
         try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            output = reader.lines().reduce("", (a, b) -> a + b + System.lineSeparator());
         }
         if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("CLI process timed out after 30 seconds");
         }
         assertEquals(expectedExitCode, process.exitValue(), output);
         return output;
      } catch (IOException | InterruptedException e) {
         throw new RuntimeException("Failed to execute the CLI process", e);
      }
   }

   @Test
   public void testHelp() {
      // The help must not require a reachable cluster, see https://github.com/infinispan/infinispan/issues/18261
      String output = run(0, "-h");
      assertThat(output).contains("Kubernetes commands");
   }

   @Test
   public void testVersion() {
      assertThat(run(0, "version")).contains("Kubernetes 1.33");
   }

   @Test
   public void testGetClusters() {
      String output = run(0, "get", "clusters");
      assertThat(output).contains("NAME", "NAMESPACE", "STATUS", "SECRETS");
      assertThat(output).contains("infinispan", NAMESPACE, "1/1");
   }

   @Test
   public void testGetClustersWithSecrets() {
      assertThat(run(0, "get", "clusters", "-s")).contains("admin", "password");
   }
}
