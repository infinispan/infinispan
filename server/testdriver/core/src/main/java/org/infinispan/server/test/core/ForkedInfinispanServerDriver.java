package org.infinispan.server.test.core;

import java.io.File;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.OS;
import org.infinispan.commons.util.Util;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @author Radoslav Husar
 * @since 11.0
 **/
public class ForkedInfinispanServerDriver extends AbstractInfinispanServerDriver {
   private static final Log log = LogFactory.getLog(ForkedInfinispanServerDriver.class);
   private static final int SHUTDOWN_TIMEOUT_SECONDS = 15;
   private final List<ForkedServer> forkedServers = new ArrayList<>();
   private final String[] serverHomes;

   protected ForkedInfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      super(configuration, InetAddress.getLoopbackAddress());
      String allServerHomes = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_SERVER_HOME);
      if (allServerHomes == null) {
         throw new IllegalArgumentException("You must specify a " + TestSystemPropertyNames.INFINISPAN_SERVER_HOME + " property pointing to a comma-separated list of server homes.");
      }
      serverHomes = allServerHomes.replaceAll("\\s+", "").split(",");
      if (serverHomes.length != configuration.numServers()) {
         throw new IllegalArgumentException("configuration.numServers should be the same " +
               "as the number of servers declared on org.infinispan.test.server");
      }
   }

   @Override
   public void prepare(String name) {
      //do nothing
   }

   @Override
   protected void start(String name, File rootDir, File configurationFile) {
      for (int i = 0; i < configuration.numServers(); i++) {
         ForkedServer server = new ForkedServer(serverHomes[i])
               .setServerConfiguration(configurationFile.getPath())
               .setPortsOffset(i);
         // Replace 99 with index of server to debug
         if (i == 99) {
            server.setJvmOptions(debugJvmOption());
         }
         copyArtifactsToUserLibDir(server.getServerLib());
         forkedServers.add(server.start());
      }
   }

   /**
    * Stop whole cluster.
    */
   @Override
   protected void stop() {
      try {
         RestResponse response = sync(getRestClient(0).cluster().stop());
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
            forkedServers.get(i).stopInternal();
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

}
