package org.infinispan.filter;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Allows AND-composing several filters.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class CompositeKeyFilter<K> implements KeyFilter<K> {
   private final KeyFilter<? super K>[] filters;

   public CompositeKeyFilter(KeyFilter<? super K>... filters) {
      this.filters = filters;
   }

   @Override
   public boolean accept(K key) {
      for (KeyFilter<? super K> k : filters)
         if (!k.accept(key)) return false;
      return true;
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      for (KeyFilter<? super K> f : filters) {
         cr.wireDependencies(f);
      }
   }

   public static class Externalizer extends AbstractExternalizer<CompositeKeyFilter> {

      @Override
      public Set<Class<? extends CompositeKeyFilter>> getTypeClasses() {
         return Util.<Class<? extends CompositeKeyFilter>>asSet(CompositeKeyFilter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, CompositeKeyFilter object) throws IOException {
         UnsignedNumeric.writeUnsignedInt(output, object.filters.length);
         for (KeyFilter filter : object.filters) {
            output.writeObject(filter);
         }
      }

      @Override
      public CompositeKeyFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int filtersSize = UnsignedNumeric.readUnsignedInt(input);
         KeyFilter[] filters = new KeyFilter[filtersSize];
         for (int i = 0; i < filtersSize; ++i) {
            filters[i] = (KeyFilter)input.readObject();
         }
         return new CompositeKeyFilter(filters);
      }

      @Override
      public Integer getId() {
         return Ids.COMPOSITE_KEY_FILTER;
      }
   }

}
