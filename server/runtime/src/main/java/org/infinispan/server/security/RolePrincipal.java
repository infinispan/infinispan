package org.infinispan.server.security;

import java.security.Principal;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RolePrincipal implements Principal {
   private final String name;

   public RolePrincipal(String name) {
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
