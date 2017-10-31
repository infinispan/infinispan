package org.infinispan.globalstate;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.filter.KeyFilter;

public class ScopeFilter implements KeyFilter<ScopedState> {
   private final String scope;

   public ScopeFilter(String scope) {
      this.scope = scope;
   }

   @Override
   public boolean accept(ScopedState key) {
      return scope.equals(key.getScope());
   }

   public static class Externalizer implements AdvancedExternalizer<ScopeFilter> {

      @Override
      public Set<Class<? extends ScopeFilter>> getTypeClasses() {
         return Collections.singleton(ScopeFilter.class);
      }

      @Override
      public Integer getId() {
         return Ids.SCOPED_STATE_FILTER;
      }

      @Override
      public void writeObject(ObjectOutput output, ScopeFilter object) throws IOException {
         output.writeUTF(object.scope);
      }

      @Override
      public ScopeFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ScopeFilter(input.readUTF());
      }
   }
}
