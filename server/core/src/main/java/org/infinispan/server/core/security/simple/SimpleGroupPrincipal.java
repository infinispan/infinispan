package org.infinispan.server.core.security.simple;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.Principal;
import java.security.acl.Group;
import java.util.Enumeration;

import org.infinispan.commons.marshall.SerializeWith;

/**
 * SimpleGroupPrincipal.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@SerializeWith(SimpleGroupPrincipal.Externalizer.class)
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

   public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<SimpleGroupPrincipal> {

      @Override
      public void writeObject(ObjectOutput output, SimpleGroupPrincipal object) throws IOException {
         output.writeUTF(object.name);
      }

      @Override
      public SimpleGroupPrincipal readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SimpleGroupPrincipal(input.readUTF());
      }

   }


}
