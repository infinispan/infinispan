package org.infinispan.globalstate;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;

/**
 * Key for scoped entries in the ClusterConfigurationManager state cache
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class ScopedState {
   private final String scope;
   private final String name;

   public ScopedState(String scope, String name) {
      this.scope = scope;
      this.name = name;
   }

   public String getScope() {
      return scope;
   }

   public String getName() {
      return name;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ScopedState that = (ScopedState) o;

      if (scope != null ? !scope.equals(that.scope) : that.scope != null) return false;
      return name != null ? name.equals(that.name) : that.name == null;
   }

   @Override
   public int hashCode() {
      int result = scope != null ? scope.hashCode() : 0;
      result = 31 * result + (name != null ? name.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "ScopedState{" +
            "scope='" + scope + '\'' +
            ", name='" + name + '\'' +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<ScopedState> {
      private static final long serialVersionUID = 326802903448963450L;

      @Override
      public Integer getId() {
         return Ids.SCOPED_STATE;
      }

      @Override
      public Set<Class<? extends ScopedState>> getTypeClasses() {
         return Collections.singleton(ScopedState.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ScopedState object) throws IOException {
         output.writeUTF(object.scope);
         output.writeUTF(object.name);
      }

      @Override
      public ScopedState readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String scope = input.readUTF();
         String name = input.readUTF();

         return new ScopedState(scope, name);
      }
   }
}
