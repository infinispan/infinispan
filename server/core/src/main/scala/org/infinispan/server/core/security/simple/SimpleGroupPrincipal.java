package org.infinispan.server.core.security.simple;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Enumeration;

/**
 * SimpleGroupPrincipal.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SimpleGroupPrincipal implements Group {

   final String name;

   public SimpleGroupPrincipal(String name) {
      this.name = name;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public boolean addMember(Principal user) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean removeMember(Principal user) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isMember(Principal member) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Enumeration<? extends Principal> members() {
      throw new UnsupportedOperationException();
   }

   @Override
   public String toString() {
      return "SimpleGroupPrincipal [name=" + name + "]";
   }

}
