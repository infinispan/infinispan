package org.infinispan.server.resp.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.resp.ExternalizerIds;

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
public class ComposedFilterConverter<K, V, C> extends AbstractKeyValueFilterConverter<K, V, C> {
   public static final AdvancedExternalizer<ComposedFilterConverter> EXTERNALIZER = new Externalizer();

   final List<KeyValueFilterConverter<K, V, C>> filterConverters;

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

   @SuppressWarnings({"unchecked", "rawtypes"})
   private static class Externalizer extends AbstractExternalizer<ComposedFilterConverter> {

      @Override
      public Integer getId() {
         return ExternalizerIds.COMPOSED_FILTER_CONVERTER;
      }

      @Override
      public Set<Class<? extends ComposedFilterConverter>> getTypeClasses() {
         return Util.asSet(ComposedFilterConverter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ComposedFilterConverter object) throws IOException {
         MarshallUtil.marshallCollection(object.filterConverters, output);
      }

      @Override
      public ComposedFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         List<KeyValueFilterConverter> filters = MarshallUtil.unmarshallCollection(input, ArrayList::new);
         return new ComposedFilterConverter(filters);
      }
   }
}
