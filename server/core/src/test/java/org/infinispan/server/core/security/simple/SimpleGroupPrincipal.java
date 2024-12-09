package org.infinispan.server.core.security.simple;

import java.security.Principal;

/**
 * SimpleGroupPrincipal.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public record SimpleGroupPrincipal(String name) implements Principal {

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String toString() {
      return "SimpleGroupPrincipal [name=" + name + "]";
   }
}
