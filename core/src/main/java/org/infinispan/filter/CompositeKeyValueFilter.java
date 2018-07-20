package org.infinispan.filter;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * Allows AND-composing several key/value filters.
 *
 * @author wburns
 * @since 7.0
 */
public class CompositeKeyValueFilter<K, V> implements KeyValueFilter<K, V> {
   private final KeyValueFilter<? super K, ? super V> filters[];

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

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      for (KeyValueFilter<? super K, ? super V> f : filters) {
         cr.wireDependencies(f);
      }
   }

   public static class Externalizer extends AbstractExternalizer<CompositeKeyValueFilter> {
      @Override
      public Set<Class<? extends CompositeKeyValueFilter>> getTypeClasses() {
         return Collections.singleton(CompositeKeyValueFilter.class);
      }

      @Override
      public void writeObject(UserObjectOutput output, CompositeKeyValueFilter object) throws IOException {
         UnsignedNumeric.writeUnsignedInt(output, object.filters.length);
         for (KeyValueFilter filter : object.filters) {
            output.writeObject(filter);
         }
      }

      @Override
      public CompositeKeyValueFilter readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         int filtersSize = UnsignedNumeric.readUnsignedInt(input);
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
