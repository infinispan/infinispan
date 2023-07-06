package org.infinispan.server.test.api;

import javax.management.MBeanServerConnection;

import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;

/**
 * @since 15.0
 **/
public class JmxTestClient {
   private final TestClient client;
   private final TestServer server;
   private String user;
   private String password;

   public JmxTestClient(TestServer server, TestClient client) {
      this.server = server;
      this.client = client;
   }

   public JmxTestClient withCredentials(String user, String password) {
      this.user = user;
      this.password = password;
      return this;
   }

   public MBeanServerConnection get(int i) {
      return server.getDriver().getJmxConnection(i, user, password, client::registerResource);
   }
}
