package org.infinispan.server.test;

import java.io.Closeable;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.test.Exceptions;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.Statement;

import net.spy.memcached.MemcachedClient;

/**
 * Creates a cluster of servers to be used for running multiple tests It performs the following tasks:
 * <ul>
 * <li>It creates a temporary directory using the test name</li>
 * <li>It creates a common configuration directory to be shared by all servers</li>
 * <li>It creates a runtime directory structure for each server in the cluster (data, log, lib)</li>
 * <li>It populates the configuration directory with multiple certificates (ca.pfx, server.pfx, user1.pfx, user2.pfx)</li>
 * </ul>
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class InfinispanServerRule implements TestRule {

   public static final Log log = LogFactory.getLog(InfinispanServerRule.class);

   private InfinispanServerDriver serverDriver;
   private InfinispanServerTestConfiguration configuration;
   protected final List<Consumer<File>> configurationEnhancers = new ArrayList<>();

   public InfinispanServerRule(InfinispanServerTestConfiguration configuration) {
      this.configuration = configuration;
   }

   /**
    * Registers a {@link Consumer} function which populates a server filesystem with additional files.
    *
    * The consumer will be invoked with the server's configuration directory
    */
   public void registerConfigurationEnhancer(Consumer<File> enhancer) {
      configurationEnhancers.add(enhancer);
   }

   public InfinispanServerDriver getServerDriver() {
      if (serverDriver == null) {
         throw new IllegalStateException("Operation not supported before test starts");
      }
      return serverDriver;
   }

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            String testName = description.getTestClass().getName();
            RunWith runWith = description.getTestClass().getAnnotation(RunWith.class);
            boolean inSuite = runWith != null && runWith.value() == Suite.class;
            if (!inSuite) {
               TestResourceTracker.testStarted(testName);
            }
            // Don't manage the server when a test is using the same InfinispanServerRule instance as the parent suite
            boolean manageServer = serverDriver == null;
            try {
               if (manageServer) {
                  serverDriver = configuration.runMode().newDriver(configuration);
                  serverDriver.prepare(testName);

                  configurationEnhancers.forEach(c -> c.accept(serverDriver.getConfDir()));

                  serverDriver.start(testName);
               }
               InfinispanServerRule.this.before(testName);

               base.evaluate();
            } finally {
               InfinispanServerRule.this.after(testName);
               if (manageServer && serverDriver != null) {
                  serverDriver.stop(testName);
               }
               if (!inSuite) {
                  TestResourceTracker.testFinished(testName);
               }
            }
         }
      };
   }

   private void before(String name) {
   }

   private void after(String name) {
   }

   /**
    * @return a client configured against the Hot Rod endpoint exposed by the server
    */
   RemoteCacheManager newHotRodClient() {
      return newHotRodClient(new ConfigurationBuilder());
   }

   /**
    * @return a client configured against the Hot Rod endpoint exposed by the server
    */
   RemoteCacheManager newHotRodClient(ConfigurationBuilder builder) {
      // Add all known server addresses
      for (int i = 0; i < getServerDriver().configuration.numServers(); i++) {
         InetSocketAddress serverAddress = getServerDriver().getServerSocket(i, 11222);
         builder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
      }
      return getServerDriver().createRemoteCacheManager(builder);
   }

   public RestClient newRestClient() {
      return newRestClient(new RestClientConfigurationBuilder());
   }

   public RestClient newRestClient(RestClientConfigurationBuilder builder) {
      // Add all known server addresses
      for (int i = 0; i < getServerDriver().configuration.numServers(); i++) {
         InetSocketAddress serverAddress = getServerDriver().getServerSocket(i, 11222);
         builder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
      }
      return RestClient.forConfiguration(builder.build());
   }

   /**
    * @param builder client configuration
    * @param n the server number
    * @return a client configured against the nth server
    */
   public RestClient newRestClient(RestClientConfigurationBuilder builder, int n) {
      InetSocketAddress serverAddress = getServerDriver().getServerSocket(n, 11222);
      builder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
      return RestClient.forConfiguration(builder.build());
   }

   /**
    * @return a client configured against the first Memcached endpoint exposed by the server
    */
   CloseableMemcachedClient newMemcachedClient() {
      List<InetSocketAddress> addresses = new ArrayList<>();
      for (int i = 0; i < getServerDriver().configuration.numServers(); i++) {
         InetSocketAddress unresolved = getServerDriver().getServerSocket(i, 11221);
         addresses.add(new InetSocketAddress(unresolved.getHostName(), unresolved.getPort()));
      }
      MemcachedClient memcachedClient = Exceptions.unchecked(() -> new MemcachedClient(addresses));
      return new CloseableMemcachedClient(memcachedClient);
   }

   public static class CloseableMemcachedClient implements Closeable {
      final MemcachedClient client;

      public CloseableMemcachedClient(MemcachedClient client) {
         this.client = client;
      }

      public MemcachedClient getClient() {
         return client;
      }

      @Override
      public void close() {
         client.shutdown();
      }
   }
}
