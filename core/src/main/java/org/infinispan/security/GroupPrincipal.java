package org.infinispan.security;

import java.security.Principal;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class GroupPrincipal implements Principal {
   private final String name;

   public GroupPrincipal(String name) {
      this.name = name;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String toString() {
      return "RolePrincipal{" +
            "name='" + name + '\'' +
            '}';
   }
}
