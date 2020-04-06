package org.infinispan.globalstate;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;

/**
 * A filter for {@link ScopedState} that allows listeners of the global state cache to choose events by scope.
 */
public class ScopeFilter implements CacheEventFilter<ScopedState, Object>, Predicate<ScopedState> {
   private final String scope;

   public ScopeFilter(String scope) {
      this.scope = scope;
   }

   @Override
   public boolean accept(ScopedState key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      return test(key);
   }

   @Override
   public boolean test(ScopedState key) {
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
