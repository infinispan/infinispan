package org.infinispan.server.test.api;

import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;

import net.spy.memcached.ClientMode;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;

/**
 *  Memcached operations for the testing framework
 *
 * @author Tristan Tarrant
 * @since 15
 */
public class MemcachedTestClientDriver extends AbstractTestClientDriver<MemcachedTestClientDriver> {
   private final TestServer testServer;
   private final TestClient testClient;
   private ConnectionFactoryBuilder builder;
   private int port = 11222; // single port

   public MemcachedTestClientDriver(TestServer testServer, TestClient testClient) {
      this.testServer = testServer;
      this.testClient = testClient;

      ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
      applyDefaultConfiguration(builder);
      this.builder = builder;
   }

   public MemcachedTestClientDriver withClientConfiguration(ConnectionFactoryBuilder builder) {
      this.builder = applyDefaultConfiguration(builder);
      return this;
   }

   public MemcachedTestClientDriver withPort(int port) {
      this.port = port;
      return this;
   }

   public MemcachedClient get() {
      return testClient.registerResource(testServer.newMemcachedClient(builder, port)).client();
   }

   @Override
   public MemcachedTestClientDriver self() {
      return this;
   }

   private ConnectionFactoryBuilder applyDefaultConfiguration(ConnectionFactoryBuilder builder) {
      if (testServer.isContainerRunWithDefaultServerConfig()) {
         // Configure admin user by default
         builder.setAuthDescriptor(AuthDescriptor.typical(TestUser.ADMIN.getUser(), TestUser.ADMIN.getPassword()));
      }
      builder.setClientMode(ClientMode.Static);
      return builder;
   }
}
