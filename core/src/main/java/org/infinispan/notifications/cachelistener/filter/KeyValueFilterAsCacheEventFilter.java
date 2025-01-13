package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * CacheEventFilter that implements it's filtering solely on the use of the provided KeyValueFilter
 *
 * @author wburns
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.KEY_VALUE_FILTER_AS_CACHE_EVENT_FILTER)
@Scope(Scopes.NONE)
public class KeyValueFilterAsCacheEventFilter<K, V> implements CacheEventFilter<K, V> {
   private final KeyValueFilter<? super K, ? super V> filter;

   public KeyValueFilterAsCacheEventFilter(KeyValueFilter<? super K, ? super V> filter) {
      this.filter = filter;
   }

   @ProtoFactory
   KeyValueFilterAsCacheEventFilter(MarshallableObject<KeyValueFilter<? super K, ? super V>> filter) {
      this.filter = MarshallableObject.unwrap(filter);
   }

   @ProtoField(number = 1)
   MarshallableObject<KeyValueFilter<? super K, ? super V>> getFilter() {
      return MarshallableObject.create(filter);
   }

   @Override
   public boolean accept(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return filter.accept(key, newValue, newMetadata);
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      cr.wireDependencies(filter);
   }
}
