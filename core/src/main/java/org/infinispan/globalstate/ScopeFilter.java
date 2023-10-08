package org.infinispan.globalstate;

import java.util.function.Predicate;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A filter for {@link ScopedState} that allows listeners of the global state cache to choose events by scope.
 */
@ProtoTypeId(ProtoStreamTypeIds.SCOPE_FILTER)
public class ScopeFilter implements CacheEventFilter<ScopedState, Object>, Predicate<ScopedState> {

   @ProtoField(1)
   final String scope;

   @ProtoFactory
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
}
