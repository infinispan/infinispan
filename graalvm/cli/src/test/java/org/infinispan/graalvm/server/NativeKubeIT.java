package org.infinispan.graalvm.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
   private static final String EXE_SUFFIX = ".exe";
   private static final long TIMEOUT_SECONDS = 30;
   private static final AtomicInteger INVOCATION = new AtomicInteger();

   private static FakeKubeApiServer server;
   private static Path workingDir;
   private static Path kubeConfig;
   private static Path unreachableKubeConfig;
   private static Path stdin;
   private static Path kubectlPlugin;
   private static Path ocPlugin;

   @BeforeAll
   public static void setup() throws IOException {
      assumeTrue(System.getProperty("infinispan.cli.bin") != null, "Requires the native CLI binary");

      workingDir = Path.of(Testing.tmpDirectory(NativeKubeIT.class));
      Util.recursiveFileRemove(workingDir.toFile());
      Files.createDirectories(workingDir);

      server = FakeKubeApiServer.start(NAMESPACE);

      kubeConfig = workingDir.resolve("kubeconfig");
      Files.writeString(kubeConfig, server.kubeConfig());

      // Port 1 is privileged and never bound, so connecting to it fails immediately rather than timing out
      unreachableKubeConfig = workingDir.resolve("kubeconfig-unreachable");
      Files.writeString(unreachableKubeConfig, server.kubeConfig().replaceFirst("http://127\\.0\\.0\\.1:\\d+", "http://127.0.0.1:1"));

      // The CLI must never block waiting for input: give it an immediate EOF
      stdin = workingDir.resolve("stdin");
      Files.createFile(stdin);

      // The CLI only enables the Kubernetes commands when it is invoked under the name of a kubectl or an oc plugin
      kubectlPlugin = installPlugin("kubectl-infinispan");
      ocPlugin = installPlugin("oc-infinispan");
   }

   private static Path installPlugin(String name) throws IOException {
      Path cli = cliBinary();
      // Windows resolves an extension-less command to <command>.exe, so the plugin must keep the suffix of the binary
      String suffix = cli.getFileName().toString().endsWith(EXE_SUFFIX) ? EXE_SUFFIX : "";
      Path plugin = workingDir.resolve(name + suffix);
      try {
         Files.createLink(plugin, cli);
      } catch (IOException | UnsupportedOperationException e) {
         Files.copy(cli, plugin);
      }
      plugin.toFile().setExecutable(true);
      return plugin;
   }

   /**
    * The failsafe configuration points at the image name, which GraalVM suffixes with <code>.exe</code> on Windows.
    */
   private static Path cliBinary() {
      Path cli = Path.of(System.getProperty("infinispan.cli.bin"));
      if (Files.exists(cli)) {
         return cli;
      }

      Path windows = cli.resolveSibling(cli.getFileName() + EXE_SUFFIX);
      assumeTrue(Files.exists(windows), "Requires the native CLI binary at " + cli + " or " + windows);
      return windows;
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
      return run(kubectlPlugin, kubeConfig, expectedExitCode, args);
   }

   private static String run(Path config, int expectedExitCode, String... args) {
      return run(kubectlPlugin, config, expectedExitCode, args);
   }

   private static String run(Path plugin, Path config, int expectedExitCode, String... args) {
      List<String> command = new ArrayList<>();
      command.add(plugin.toString());
      command.addAll(Arrays.asList(args));
      // The output is redirected to a file rather than drained from a pipe: reading a pipe blocks until the process
      // closes its output, which would defeat the timeout below if the CLI ever hangs
      Path log = workingDir.resolve("cli-" + INVOCATION.incrementAndGet() + ".log");
      ProcessBuilder pb = new ProcessBuilder(command);
      pb.environment().put("KUBECONFIG", config.toString());
      pb.redirectErrorStream(true);
      pb.redirectInput(stdin.toFile());
      pb.redirectOutput(log.toFile());
      Process process = null;
      try {
         process = pb.start();
         if (!process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException("CLI process timed out after " + TIMEOUT_SECONDS + " seconds: " + read(log));
         }
         String output = read(log);
         assertEquals(expectedExitCode, process.exitValue(), output);
         return output;
      } catch (IOException e) {
         throw new UncheckedIOException("Failed to execute the CLI process", e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new IllegalStateException("Interrupted while waiting for the CLI process", e);
      } finally {
         if (process != null && process.isAlive()) {
            process.destroyForcibly();
         }
      }
   }

   private static String read(Path log) {
      try {
         return Files.exists(log) ? Files.readString(log, StandardCharsets.UTF_8) : "";
      } catch (IOException e) {
         throw new UncheckedIOException("Failed to read the CLI output", e);
      }
   }

   @Test
   public void testHelp() {
      // The help must not require a reachable cluster, see https://github.com/infinispan/infinispan/issues/18261
      String output = run(unreachableKubeConfig, 0, "-h");
      assertThat(output).contains("Kubernetes commands");
   }

   @Test
   public void testOpenShiftPlugin() {
      // oc runs its plugins as oc-<plugin>, so the Kubernetes commands must be enabled under that name too
      assertThat(run(ocPlugin, unreachableKubeConfig, 0, "-h")).contains("Kubernetes commands");
      assertThat(run(ocPlugin, kubeConfig, 0, "get", "clusters")).contains("infinispan", NAMESPACE);
   }

   @Test
   public void testVersion() {
      assertThat(run(0, "version")).contains("Kubernetes 1.33");
   }

   @Test
   public void testVersionWithoutCluster() {
      // The CLI version must be reported even when the cluster cannot be reached
      String output = run(unreachableKubeConfig, 0, "version");
      assertThat(output).contains("CLI:");
      assertThat(output).doesNotContain("Kubernetes 1.33");
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
