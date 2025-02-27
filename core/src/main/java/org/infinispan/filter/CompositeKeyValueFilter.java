package org.infinispan.filter;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.protostream.impl.MarshallableArray;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Allows AND-composing several key/value filters.
 *
 * @author wburns
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COMPOSITE_KEY_VALUE_FILTER)
@Scope(Scopes.NONE)
public class CompositeKeyValueFilter<K, V> implements KeyValueFilter<K, V> {
   private final MarshallableArray<KeyValueFilter<? super K, ? super V>> filters;

   public CompositeKeyValueFilter(KeyValueFilter<? super K, ? super V>... filters) {
      this.filters = MarshallableArray.create(filters);
   }

   @ProtoFactory
   @SuppressWarnings("unchecked")
   CompositeKeyValueFilter(MarshallableArray<KeyValueFilter<? super K, ? super V>> filters) {
      this.filters = filters;
   }

   @ProtoField(1)
   MarshallableArray<KeyValueFilter<? super K, ? super V>> getFilters() {
      return filters;
   }

   @Override
   public boolean accept(K key, V value, Metadata metadata) {
      for (KeyValueFilter<? super K, ? super V> filter : MarshallableArray.unwrap(filters)) {
         if (!filter.accept(key, value, metadata)) {
            return false;
         }
      }
      return true;
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      for (KeyValueFilter<? super K, ? super V> f : MarshallableArray.unwrap(filters)) {
         cr.wireDependencies(f);
      }
   }
}
