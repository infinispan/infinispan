package org.infinispan.server.core.security.simple;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.server.core.security.UserPrincipal;

/**
 * SimpleUserPrincipal.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */

@SerializeWith(SimpleUserPrincipal.Externalizer.class)
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

   public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<SimpleUserPrincipal> {

      @Override
      public void writeObject(ObjectOutput output, SimpleUserPrincipal object) throws IOException {
         output.writeUTF(object.name);
      }

      @Override
      public SimpleUserPrincipal readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SimpleUserPrincipal(input.readUTF());
      }

   }
}
