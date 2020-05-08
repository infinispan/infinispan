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
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;

/**
 * @author Gustavo Lira &lt;glira@redhat.com&gt;
 * @since 11.0
 **/
public class ForkedInfinispanServerDriver extends AbstractInfinispanServerDriver {
   private static final Log log = org.infinispan.commons.logging.LogFactory.getLog(ForkedInfinispanServerDriver.class);
   private List<ForkedServer> forkedServers = new ArrayList<>();


   protected ForkedInfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      super(configuration, InetAddress.getLoopbackAddress());
   }

   @Override
   public void prepare(String name) {
      //do nothing
   }

   @Override
   public void start(String name) {
      log.infof("Starting server %s", name);
      start(name, null, configuration.configurationFile());
      log.infof("Started server %s", name);
   }

   @Override
   protected void start(String name, File rootDir, String configurationFile) {
      String allServerHomes = System.getProperty(TestSystemPropertyNames.INFINISPAN_SERVER_HOME);
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
               .setServerConfiguration(configurationFile)
               .setPortsOffset(i)
               .start();
         forkedServers.add(server);
         copyArtifactsToUserLibDir(server.getServerLib());
         forkedServers.get(0).printServerLog(s -> log.info(s));
      }
   }

   @Override
   /**
    * Stop all cluster
    */
   protected void stop() {
      sync(getRestClient(0).cluster().stop());
   }

   @Override
   /**
    * Stop a specific server
    */
   public void stop(int server) {
      sync(getRestClient(0).server().stop());
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
      return new InetSocketAddress(getServerAddress(server), getServerPort(server));
   }

   @Override
   public InetAddress getServerAddress(int server) {
      return Exceptions.unchecked(() -> InetAddress.getByName("localhost"));
   }

   @Override
   public void pause(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void resume(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void kill(int server) {
      new ProcessBuilder("kill -9 " + forkedServers.get(server).getPid());
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
      builder.addServer().host("localhost").port(getServerPort(server));
      return RestClient.forConfiguration(builder.build());
   }

   private int getServerPort(int server) {
      return server == 0 ? ForkedServer.DEFAULT_SINGLE_PORT : ForkedServer.OFFSET_FACTOR * server + ForkedServer.DEFAULT_SINGLE_PORT;
   }

   private static <T> T sync(CompletionStage<T> stage) {
      return Exceptions.unchecked(() -> stage.toCompletableFuture().get(5, TimeUnit.SECONDS));
   }

}
