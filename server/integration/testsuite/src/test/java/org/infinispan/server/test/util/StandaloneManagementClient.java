package org.infinispan.server.test.util;

import org.wildfly.extras.creaper.core.online.ModelNodeResult;
import org.wildfly.extras.creaper.core.online.OnlineManagementClient;
import org.wildfly.extras.creaper.core.online.OnlineOptions;
import org.wildfly.extras.creaper.core.online.operations.Address;
import org.wildfly.extras.creaper.core.online.operations.Operations;

import java.io.IOException;

/**
 * A version of {@link ManagementClient) to be used for standalone servers.
 *
 * @author mgencur
 */
public class StandaloneManagementClient {

    public static final String LOGIN = System.getProperty("login", "admin");
    public static final String PASSWORD = System.getProperty("password", "admin9Pass!");

    public String nodeName;
    private Operations ops;

    public StandaloneManagementClient(String mgmtAddress, int mgmtPort, String nodeName) {
       this.nodeName = nodeName;
        OnlineManagementClient onlineClient;
        try {
           onlineClient = org.wildfly.extras.creaper.core.ManagementClient.online(OnlineOptions.standalone()
                             .hostAndPort(mgmtAddress, mgmtPort)
                             .auth(LOGIN, PASSWORD)
                             .build()
                            );
       } catch (IOException ex) {
           throw new IllegalStateException("Error during connecting to server CLI.", ex);
       }
       ops = new Operations(onlineClient);
    }

   public String getInfinispanView() {
      ModelNodeResult result;
      Address addressRoot = Address.root();
      try {
         result = ops.readAttribute(
               addressRoot.
                     and("subsystem", "datagrid-infinispan").and("cache-container", "clustered"), "members");
      } catch (IOException ex) {
         throw new IllegalStateException("Error executing operation.", ex);
      }

      return result.get("result").asString();
   }

}
