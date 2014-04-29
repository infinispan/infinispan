package org.infinispan.server.core.security.simple;

import org.infinispan.server.core.security.UserPrincipal;

/**
 * SimpleUserPrincipal.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public final class SimpleUserPrincipal implements UserPrincipal {
   private final String name;

   public SimpleUserPrincipal(final String name) {
      if (name == null) {
         throw new IllegalArgumentException("name is null");
      }
      this.name = name;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public int hashCode() {
      return name.hashCode();
   }

   @Override
   public boolean equals(Object other) {
      return other instanceof SimpleUserPrincipal && equals((SimpleUserPrincipal) other);
   }

   public boolean equals(SimpleUserPrincipal other) {
      return this == other || other != null && name.equals(other.name);
   }

   @Override
   public String toString() {
      return "SimpleUserPrincipal [name=" + name + "]";
   }
}
