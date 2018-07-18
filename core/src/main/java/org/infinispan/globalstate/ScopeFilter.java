package org.infinispan.globalstate;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.filter.KeyFilter;

public class ScopeFilter implements KeyFilter<ScopedState>, Predicate<ScopedState> {
   private final String scope;

   public ScopeFilter(String scope) {
      this.scope = scope;
   }

   @Override
   public boolean accept(ScopedState key) {
      return scope.equals(key.getScope());
   }

   @Override
   public boolean test(ScopedState key) {
      return accept(key);
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
      public void writeObject(UserObjectOutput output, ScopeFilter object) throws IOException {
         output.writeUTF(object.scope);
      }

      @Override
      public ScopeFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ScopeFilter(input.readUTF());
      }
   }
}
