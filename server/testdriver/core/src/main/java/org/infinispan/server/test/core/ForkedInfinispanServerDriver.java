package org.infinispan.server.test.core;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;

import org.apache.commons.io.FileUtils;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.OS;
import org.infinispan.commons.util.Util;
import org.infinispan.server.Server;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @author Radoslav Husar
 * @since 11.0
 **/
public class ForkedInfinispanServerDriver extends AbstractInfinispanServerDriver {
   private static final Log log = LogFactory.getLog(ForkedInfinispanServerDriver.class);
   private static final int SHUTDOWN_TIMEOUT_SECONDS = 15;
   private final List<ForkedServer> forkedServers = new ArrayList<>();
   private final List<Path> serverHomes;
   private final String serverDataPath;

   protected ForkedInfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      super(configuration, InetAddress.getLoopbackAddress());
      String globalServerHome = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR);
      if (globalServerHome == null || globalServerHome.isEmpty()) {
         throw new IllegalArgumentException("You must specify a " + TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR + " property.");
      }
      serverDataPath = System.getProperty(Server.INFINISPAN_SERVER_ROOT_PATH);
      if (serverDataPath != null && !serverDataPath.trim().isEmpty() && !serverDataPath.contains("%d")) {
         throw new IllegalStateException("Server root path should have the index. Add the %d regex to the path. Example: /path/to/server_%d");
      }
      this.serverHomes = new ArrayList<>();
      Path src = Paths.get(globalServerHome).normalize();
      copyServer(src, serverDataPath != null ? 1 : configuration.numServers());
   }

   private void copyServer(Path src, int maxServers) {
      for (int i = 0; i < maxServers; i++) {
         Path dest = Paths.get(CommonsTestingUtil.tmpDirectory(), Util.threadLocalRandomUUID().toString());
         try {
            Files.createDirectory(dest);
            Files.walkFileTree(src, new CommonsTestingUtil.CopyFileVisitor(dest, true));
         } catch (IOException e) {
            throw new UncheckedIOException("Cannot copy the server to temp folder", e);
         }
         serverHomes.add(dest);
      }
   }

   private Path getServerConfDir(String home) {
      return Paths.get(home,"server", "conf");
   }

   @Override
   public void prepare(String name) {
      //do nothing
   }

   @Override
   protected void start(String name, File rootDir, File configurationFile) {
      for (int i = 0; i < configuration.numServers(); i++) {
         ForkedServer server;
         Path destConfDir;
         if (serverDataPath != null) {
            String serverHome = serverHomes.get(0).toString();
            File serverRootPath = new File(String.format(serverDataPath, i));
            createServerStructure(serverHome, serverRootPath);
            destConfDir = getServerConfDir(serverRootPath.getAbsolutePath());
            server = new ForkedServer(serverHome);
            server.addSystemProperty(Server.INFINISPAN_SERVER_ROOT_PATH, serverRootPath);
         } else {
            String serverHome = serverHomes.get(i).toString();
            destConfDir = getServerConfDir(serverHome);
            server = new ForkedServer(serverHome);
         }
         server
               .setServerConfiguration(configurationFile.getPath())
               .setPortsOffset(i);
         server.addSystemProperty(Server.INFINISPAN_CLUSTER_STACK, System.getProperty(Server.INFINISPAN_CLUSTER_STACK));
         try {
            File sourceServerConfiguration = new File(server.getServerConfiguration());
            FileUtils.copyFile(sourceServerConfiguration, destConfDir.resolve(sourceServerConfiguration.getName()).toFile());
         } catch (IOException e) {
            throw new UncheckedIOException("Cannot copy the server to temp directory", e);
         }
         // Replace 99 with index of server to debug
         if (i == 99) {
            server.setJvmOptions(debugJvmOption());
         }
         copyArtifactsToUserLibDir(server.getServerLib());
         forkedServers.add(server.start());
      }
   }

   private void createServerStructure(String serverHome, File serverRootPath) {
      // copy required files
      try {
         FileUtils.deleteDirectory(serverRootPath);
         FileUtils.copyDirectory(getServerConfDir(serverHome).getParent().toFile(), serverRootPath,
               p -> !p.isDirectory() || !p.getName().equals("data") || !p.getName().equals("log"));
      } catch (IOException e) {
         throw new UncheckedIOException("Cannot copy the default values", e);
      }
   }

   /**
    * Stop whole cluster.
    */
   @Override
   protected void stop() {
      try {
         // check if the server is running
         try (RestClient restClient = getRestClient(0)) {
            RestResponse response = sync(restClient.cluster().stop());
            // Ensure non-error response code from the REST endpoint.
            if (response.getStatus() >= 400) {
               throw new IllegalStateException(String.format("Failed to shutdown the cluster gracefully, got status %d.", response.getStatus()));
            } else {
               // Ensure that the server process has really quit
               // - if it has; the getPid will throw an exception
               boolean javaProcessQuit = false;
               long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(SHUTDOWN_TIMEOUT_SECONDS);
               while (!(javaProcessQuit || endTime < System.currentTimeMillis())) {
                  try {
                     forkedServers.get(0).getPid();
                     Thread.sleep(500);
                  } catch (IllegalStateException ignore) {
                     // The process has quit.
                     javaProcessQuit = true;
                  }
               }
               if (!javaProcessQuit) {
                  throw new IllegalStateException("Server Java process has not gracefully quit within " + SHUTDOWN_TIMEOUT_SECONDS + " seconds.");
               }
            }
         } catch (RuntimeException e) {
            log.warn("Server is not running", e);
         }
      } catch (Exception e) {
         // kill the servers
         log.error("Got exception while gracefully shutting down the cluster. Killing the servers.", e);
         for (int i = 0; i < configuration.numServers(); i++) {
            try {
               kill(i);
            } catch (Exception exception) {
               log.errorf("Failed to kill server #%d, exception was: %s", i, exception);
            }
         }
      } finally {
         for (int i = 0; i < configuration.numServers(); i++) {
            // Do an internal stop - e.g. stops the log monitoring process.
            if (i < forkedServers.size()) {
               forkedServers.get(i).stopInternal();
            }
         }
      }
   }

   /**
    * Stop a specific server
    */
   @Override
   public void stop(int server) {
      sync(getRestClient(server).server().stop());
   }

   @Override
   public boolean isRunning(int server) {
      try {
         sync(getRestClient(server).server().configuration());
      } catch (RuntimeException r) {
         return !(Util.getRootCause(r) instanceof ConnectException);
      }
      return true;
   }

   @Override
   public InetSocketAddress getServerSocket(int server, int port) {
      return new InetSocketAddress(getServerAddress(server), getServerPort(server, port));
   }

   @Override
   public InetAddress getServerAddress(int server) {
      return Exceptions.unchecked(() -> InetAddress.getByName("localhost"));
   }

   @Override
   public void pause(int server) {
      if (OS.getCurrentOs() == OS.WINDOWS) {
         throw new UnsupportedOperationException("Forked mode does not support pause on Windows!");
      } else {
         Exceptions.unchecked(() -> new ProcessBuilder("kill", "-SIGSTOP", String.valueOf(forkedServers.get(server).getPid())).start().waitFor(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      }
   }

   @Override
   public void resume(int server) {
      if (OS.getCurrentOs() == OS.WINDOWS) {
         throw new UnsupportedOperationException("Forked mode does not support resume on Windows!");
      } else {
         Exceptions.unchecked(() -> new ProcessBuilder("kill", "-SIGCONT", String.valueOf(forkedServers.get(server).getPid())).start().waitFor(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      }
   }

   @Override
   public void kill(int server) {
      String pid = String.valueOf(forkedServers.get(server).getPid());
      if (OS.getCurrentOs() == OS.WINDOWS) {
         Exceptions.unchecked(() -> new ProcessBuilder("TASKKILL", "/PID", pid, "/F", "/T").start().waitFor(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      } else {
         Exceptions.unchecked(() -> new ProcessBuilder("kill", "-9", pid).start().waitFor(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      }
   }

   @Override
   public void restart(int server) {
      throw new UnsupportedOperationException("Forked mode does not support restart!");
   }

   @Override
   public void restartCluster() {
      throw new UnsupportedOperationException("Forked mode does not support cluster restart!");
   }

   @Override
   public MBeanServerConnection getJmxConnection(int server) {
      return null;
   }

   @Override
   public int getTimeout() {
      return ForkedServer.TIMEOUT_SECONDS;
   }

   private RestClient getRestClient(int server) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();

      // Filter driver properties for REST client configuration properties, e.g. security configuration
      // and apply them before applying rest of the dynamically created configuration, e.g. port.
      Properties securityConfigurationProperties = new Properties();
      configuration.properties().entrySet().stream()
              // Filters for org.infinispan.client.rest.configuration.RestClientConfigurationProperties#ICR field value.
              .filter(entry -> entry.getKey().toString().startsWith("infinispan.client.rest."))
              .forEach(entry -> securityConfigurationProperties.put(entry.getKey(), entry.getValue()));
      builder.withProperties(securityConfigurationProperties);
      // Ensure to not print out the *values*!!!
      log.debugf("Configured client with the following properties: %s", securityConfigurationProperties.keySet().toString());

      builder.addServer().host("localhost").port(getServerPort(server, ForkedServer.DEFAULT_SINGLE_PORT));
      return RestClient.forConfiguration(builder.build());
   }

   private int getServerPort(int server, int port) {
      return port + ForkedServer.OFFSET_FACTOR * server;
   }

   private static <T> T sync(CompletionStage<T> stage) {
      return Exceptions.unchecked(() -> stage.toCompletableFuture().get(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS));
   }

   @Override
   public String syncFilesFromServer(int server, String dir) {
      //NO-OP
      return getRootDir().toPath().resolve(Integer.toString(server)).toString();
   }

   @Override
   public String syncFilesToServer(int server, String path) {
      //NO-OP
      return path;
   }
}
