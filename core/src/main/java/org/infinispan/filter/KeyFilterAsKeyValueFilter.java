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
 * KeyValueFilter that implements it's filtering solely on the use of the provided KeyFilter
 *
 * @author wburns
 * @since 7.0
 */
public class KeyFilterAsKeyValueFilter<K, V> implements KeyValueFilter<K, V> {
   private final KeyFilter<? super K> filter;

   public KeyFilterAsKeyValueFilter(KeyFilter<? super K> filter) {
      if (filter == null) {
         throw new NullPointerException();
      }
      this.filter = filter;
   }
   @Override
   public boolean accept(K key, V value, Metadata metadata) {
      return filter.accept(key);
   }

   public static class Externalizer extends AbstractExternalizer<KeyFilterAsKeyValueFilter> {
      @Override
      public Set<Class<? extends KeyFilterAsKeyValueFilter>> getTypeClasses() {
         return Util.<Class<? extends KeyFilterAsKeyValueFilter>>asSet(KeyFilterAsKeyValueFilter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, KeyFilterAsKeyValueFilter object) throws IOException {
         output.writeObject(object.filter);
      }

      @Override
      public KeyFilterAsKeyValueFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new KeyFilterAsKeyValueFilter((KeyFilter)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.KEY_FILTER_AS_KEY_VALUE_FILTER;
      }
   }
}
