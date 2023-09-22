package org.infinispan.server.resp.filter;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

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
@ProtoName("ComposedFilterConverter")
public class ComposedFilterConverter<K, V, C> extends AbstractKeyValueFilterConverter<K, V, C> {

   @ProtoField(collectionImplementation = ArrayList.class)
   final List<KeyValueFilterConverter<K, V, C>> filterConverters;

   @ProtoFactory
   public ComposedFilterConverter(List<KeyValueFilterConverter<K, V, C>> filterConverters) {
      this.filterConverters = filterConverters;
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
