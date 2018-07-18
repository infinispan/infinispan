package org.infinispan.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.Ids;

/**
 * This is a KeyFilter that utilizes the given {@link org.infinispan.filter.KeyValueFilter} to determine if to
 * filter the key.  Note this filter will be passed null for both the value and metadata on every pass.
 *
 * @author wburns
 * @since 7.0
 */
public class KeyValueFilterAsKeyFilter<K> implements KeyFilter<K> {

   private final KeyValueFilter<? super K, ?> filter;

   public KeyValueFilterAsKeyFilter(KeyValueFilter<? super K, ?> filter) {
      this.filter = filter;
   }

   @Override
   public boolean accept(K key) {
      return filter.accept(key, null, null);
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      cr.wireDependencies(filter);
   }

   public static class Externalizer extends AbstractExternalizer<KeyValueFilterAsKeyFilter> {

      @Override
      public Set<Class<? extends KeyValueFilterAsKeyFilter>> getTypeClasses() {
         return Collections.singleton(KeyValueFilterAsKeyFilter.class);
      }

      @Override
      public void writeObject(UserObjectOutput output, KeyValueFilterAsKeyFilter object) throws IOException {
         output.writeObject(object.filter);
      }

      @Override
      public KeyValueFilterAsKeyFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new KeyValueFilterAsKeyFilter((KeyValueFilter) input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.KEY_VALUE_FILTER_AS_KEY_FILTER;
      }
   }
}
