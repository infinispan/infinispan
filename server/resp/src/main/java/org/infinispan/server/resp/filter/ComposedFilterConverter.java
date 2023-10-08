package org.infinispan.server.resp.filter;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A filter which is composed of other filters.
 * <p>
 * The operations are applied to the underlying filters in no specific order. Once the first filter returns
 * <code>null</code>, the execution stops.
 *
 * @param <K>: The filters key types.
 * @param <V>: The filters value types.
 * @param <C>: The filters return type.
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_COMPOSED_FILTER_CONVERTER)
public class ComposedFilterConverter<K, V, C> extends AbstractKeyValueFilterConverter<K, V, C> {

   final List<KeyValueFilterConverter<K, V, C>> filterConverters;

   public ComposedFilterConverter(List<KeyValueFilterConverter<K, V, C>> filterConverters) {
      this.filterConverters = filterConverters;
   }

   @ProtoFactory
   ComposedFilterConverter(MarshallableCollection<KeyValueFilterConverter<K, V, C>> filterConverters) {
      this.filterConverters = MarshallableCollection.unwrap(filterConverters, ArrayList::new);
   }

   @ProtoField(1)
   MarshallableCollection<KeyValueFilterConverter<K, V, C>> getFilterConverters() {
      return MarshallableCollection.create(filterConverters);
   }

   @Override
   public C filterAndConvert(K key, V value, Metadata metadata) {
      C ret = null;
      for (KeyValueFilterConverter<K, V, C> fc : filterConverters) {
         ret = fc.filterAndConvert(key, value, metadata);
         if (ret == null) break;
      }
      return ret;
   }

   @Override
   public MediaType format() {
      return null;
   }
}
