package org.infinispan.server.test.api;

import java.util.Properties;

import org.infinispan.cli.Context;
import org.infinispan.cli.commands.CLI;
import org.infinispan.cli.impl.AeshDelegatingShell;
import org.infinispan.server.test.core.CliConnection;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.TestClient;

public class CliTestDriver extends AbstractTestClientDriver<CliTestDriver> {
   private final String hostAddress;
   private final TestClient testClient;
   private CliConnection connection;
   private int port = 11222;
   private TestUser user = TestUser.ADMIN;
   private final Properties properties;
   private String[] args;

   public CliTestDriver(InfinispanServerDriver driver, TestClient testClient) {
      this.testClient = testClient;
      properties = new Properties(System.getProperties());
      properties.put("cli.dir", driver.getRootDir().getAbsolutePath());
      properties.put(Context.Property.AUTOSUGGEST.propertyName(), "false");
      hostAddress = driver.getServerAddress(0).getHostAddress();
   }

   public CliTestDriver(InfinispanServerDriver driver) {
      this(driver, new TestClient(null).initResources());
   }

   public CliTestDriver withArguments(String... args) {
      this.args = args;
      return this;
   }

   public CliTestDriver withPort(int port) {
      this.port = port;
      return this;
   }

   public CliTestDriver withUser(TestUser user) {
      this.user = user;
      return this;
   }

   public CliTestDriver withProperty(String key, String value) {
      properties.put(key, value);
      return this;
   }

   public CliConnection connect() {
      connection(); // Ensure the connection has been created
      connection.send("connect " + url());
      connection.assertContains("//containers/default]>");
      connection.clear();
      return connection;
   }

   public CliConnection connection() {
      if (connection == null) {
         connection = testClient.registerResource(new CliConnection());
         CLI.main(new AeshDelegatingShell(connection), properties, args);
      }
      return connection;
   }

   public String url() {
      return String.format("http://%s:%s@%s:%d", user.getUser(), user.getPassword(), hostAddress, port);
   }

   @Override
   public CliTestDriver self() {
      return this;
   }
}
