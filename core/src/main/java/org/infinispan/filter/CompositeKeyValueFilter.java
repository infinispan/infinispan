package org.infinispan.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Allows AND-composing several key/value filters.
 *
 * @author wburns
 * @since 7.0
 */
public class CompositeKeyValueFilter<K, V> implements KeyValueFilter<K, V> {
   KeyValueFilter<? super K, ? super V> filters[];

   public CompositeKeyValueFilter(KeyValueFilter<? super K, ? super V>... filters) {
      this.filters = filters;
   }


   @Override
   public boolean accept(K key, V value, Metadata metadata) {
      for (KeyValueFilter<? super K, ? super V> filter : filters) {
         if (!filter.accept(key, value, metadata)) {
            return false;
         }
      }
      return true;
   }

   public static class Externalizer extends AbstractExternalizer<CompositeKeyValueFilter> {
      @Override
      public Set<Class<? extends CompositeKeyValueFilter>> getTypeClasses() {
         return Util.<Class<? extends CompositeKeyValueFilter>>asSet(CompositeKeyValueFilter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, CompositeKeyValueFilter object) throws IOException {
         output.writeInt(object.filters.length);
         for (KeyValueFilter filter : object.filters) {
            output.writeObject(filter);
         }
      }

      @Override
      public CompositeKeyValueFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int filtersSize = input.readInt();
         KeyValueFilter[] filters = new KeyValueFilter[filtersSize];
         for (int i = 0; i < filtersSize; ++i) {
            filters[i] = (KeyValueFilter)input.readObject();
         }
         return new CompositeKeyValueFilter(filters);
      }

      @Override
      public Integer getId() {
         return Ids.COMPOSITE_KEY_VALUE_FILTER;
      }
   }
}
