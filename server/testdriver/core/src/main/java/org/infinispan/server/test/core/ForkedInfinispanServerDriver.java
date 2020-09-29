package org.infinispan.server.test.core;

import java.io.File;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @author Radoslav Husar
 * @since 11.0
 **/
public class ForkedInfinispanServerDriver extends AbstractInfinispanServerDriver {
   private static final Log log = org.infinispan.commons.logging.LogFactory.getLog(ForkedInfinispanServerDriver.class);
   private final List<ForkedServer> forkedServers = new ArrayList<>();

   protected ForkedInfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      super(configuration, InetAddress.getLoopbackAddress());
   }

   @Override
   public void prepare(String name) {
      //do nothing
   }

   @Override
   protected void start(String name, File rootDir, File configurationFile) {
      String allServerHomes = configuration.properties().getProperty(TestSystemPropertyNames.INFINISPAN_SERVER_HOME);
      if (allServerHomes == null) {
         throw new IllegalArgumentException("You must specify a " + TestSystemPropertyNames.INFINISPAN_SERVER_HOME + " property pointing to a comma-separated list of server homes.");
      }
      String[] serverHomes = allServerHomes.replaceAll("\\s+", "").split(",");
      if (serverHomes.length != configuration.numServers()) {
         throw new IllegalArgumentException("configuration.numServers should be the same " +
               "as the number of servers declared on org.infinispan.test.server");
      }
      for (int i = 0; i < configuration.numServers(); i++) {
         ForkedServer server = new ForkedServer(serverHomes[i])
               .setServerConfiguration(configurationFile.getPath())
               .setPortsOffset(i);
         copyArtifactsToUserLibDir(server.getServerLib());
         forkedServers.add(server.start());
         forkedServers.get(0).printServerLog(log::info);
      }
   }

   /**
    * Stop all cluster
    */
   @Override
   protected void stop() {
      try {
         RestResponse response = sync(getRestClient(0).cluster().stop());
         // Ensure non-error response code from the REST endpoint.
         if (response.getStatus() >= 400) {
            log.errorf("Failed to shutdown the cluster gracefully, got status %d.", response.getStatus());
            throw new IllegalStateException();
         }
      } catch (Exception e) {
         // kill the servers
         log.error("Got exception while gracefully shutting down the cluster. Killing the servers.", e);
         for (int i = 0; i < configuration.numServers(); i++) {
            kill(i);
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
      Exceptions.unchecked(() -> new ProcessBuilder("kill", "-SIGSTOP", String.valueOf(forkedServers.get(server).getPid())).start().waitFor(10, TimeUnit.SECONDS));
   }

   @Override
   public void resume(int server) {
      Exceptions.unchecked(() -> new ProcessBuilder("kill", "-SIGCONT", String.valueOf(forkedServers.get(server).getPid())).start().waitFor(10, TimeUnit.SECONDS));
   }

   @Override
   public void kill(int server) {
      Exceptions.unchecked(() -> new ProcessBuilder("kill", "-9", String.valueOf(forkedServers.get(server).getPid())).start().waitFor(10, TimeUnit.SECONDS));
   }

   @Override
   public void restart(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void restartCluster() {
      throw new UnsupportedOperationException();
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
      builder.addServer().host("localhost").port(getServerPort(server, ForkedServer.DEFAULT_SINGLE_PORT));
      return RestClient.forConfiguration(builder.build());
   }

   private int getServerPort(int server, int port) {
      return server == 0 ? port : ForkedServer.OFFSET_FACTOR * server + port;
   }

   private static <T> T sync(CompletionStage<T> stage) {
      return Exceptions.unchecked(() -> stage.toCompletableFuture().get(5, TimeUnit.SECONDS));
   }

}
