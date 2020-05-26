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
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.test.CommonsTestingUtil;
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
   protected void start(String name, File rootDir, File configurationFile) {
      if (configuration.isParallelStartup()) {
         throw new IllegalStateException("Forked mode doesn't support parallel startup");
      }
      String globalServerHome = System.getProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR);
      if (globalServerHome == null) {
         throw new IllegalArgumentException("You must specify a " + TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR + " property.");
      }

      List<Path> serverHomes = new ArrayList<>();
      Path src = Paths.get(globalServerHome).normalize();
      for (int i = 0; i < configuration.numServers(); i++) {
         Path dest = Paths.get(CommonsTestingUtil.tmpDirectory(), UUID.randomUUID().toString());
         try {
            Files.createDirectory(dest);
            Files.walkFileTree(src, new CommonsTestingUtil.CopyFileVisitor(dest, true));
         } catch (IOException e) {
            throw new UncheckedIOException("Cannot copy the server to temp folder", e);
         }
         serverHomes.add(dest);
      }

      if (serverHomes.size() != configuration.numServers()) {
         throw new IllegalArgumentException("configuration.numServers should be the same " +
               "as the number of servers declared on org.infinispan.test.server");
      }
      for (int i = 0; i < configuration.numServers(); i++) {
         String serverHome = serverHomes.get(i).toString();
         ForkedServer server = new ForkedServer(serverHome)
               .setServerConfiguration(configurationFile.getPath())
               .setPortsOffset(i);
         copyArtifactsToUserLibDir(server.getServerLib());
         forkedServers.add(server.start());
         forkedServers.get(0).printServerLog(s -> log.info(s));
      }
   }

   @Override
   /**
    * Stop all cluster
    */
   protected void stop() {
      try {
         sync(getRestClient(0).cluster().stop());
      } catch (Exception e) {
         // kill the servers
         log.error("Could not gracefully shutdown the cluster. Killing the servers.", e);
         for (int i =0 ; i < configuration.numServers(); i++) {
            kill(i);
         }
      }
   }

   @Override
   /**
    * Stop a specific server
    */
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
      Exceptions.unchecked(() -> new ProcessBuilder("kill -SIGSTOP " + forkedServers.get(server).getPid()).start().waitFor(10, TimeUnit.SECONDS));
   }

   @Override
   public void resume(int server) {
      Exceptions.unchecked(() -> new ProcessBuilder("kill -SIGCONT " + forkedServers.get(server).getPid()).start().waitFor(10, TimeUnit.SECONDS));
   }

   @Override
   public void kill(int server) {
      Exceptions.unchecked(() -> new ProcessBuilder("kill -9 " + forkedServers.get(server).getPid()).start().waitFor(10, TimeUnit.SECONDS));
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
      return server == 0 ?  port : ForkedServer.OFFSET_FACTOR * server + port;
   }

   private static <T> T sync(CompletionStage<T> stage) {
      return Exceptions.unchecked(() -> stage.toCompletableFuture().get(5, TimeUnit.SECONDS));
   }

}
