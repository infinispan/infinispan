package org.infinispan.server.security;

import java.util.Collections;

import org.infinispan.cli.user.UserTool;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerListener;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class SecurityRealmServerListener implements InfinispanServerListener {

   private final String realm;

   public SecurityRealmServerListener(String realmName) {
      this.realm = realmName;
   }

   @Override
   public void before(InfinispanServerDriver driver) {
      UserTool userTool = new UserTool(driver.getRootDir().getAbsolutePath(), realm + "-users.properties", realm + "-groups.properties");
      // Create users and groups for individual permissions
      for (AuthorizationPermission permission : AuthorizationPermission.values()) {
         String name = permission.name().toLowerCase();
         userTool.createUser(username(name + "_user"), name, realm, UserTool.Encryption.DEFAULT, Collections.singletonList(name), null);
      }
      // Create users with composite roles
      for (TestUser user : TestUser.values()) {
         userTool.createUser(username(user.getUser()), user.getPassword(), realm, UserTool.Encryption.DEFAULT, user.getRoles(), null);
      }
   }

   private String username(String name) {
      return realm + "_" + name;
   }
}
