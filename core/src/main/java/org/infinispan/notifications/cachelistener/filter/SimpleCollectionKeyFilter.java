package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.notifications.KeyFilter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Set;

/**
 * Simple collection based key filter that checks if the provided collection contains the key.
 *
 * @author wburns
 * @since 7.0
 */
public class SimpleCollectionKeyFilter<K> implements KeyFilter<K> {
   private final Collection<? extends K> collection;

   public SimpleCollectionKeyFilter(Collection<? extends K> collection) {
      this.collection = collection;
   }
   @Override
   public boolean accept(K key) {
      return collection.contains(key);
   }

   public static class Externalizer extends AbstractExternalizer<SimpleCollectionKeyFilter> {
      @Override
      public Set<Class<? extends SimpleCollectionKeyFilter>> getTypeClasses() {
         return Util.<Class<? extends SimpleCollectionKeyFilter>>asSet(SimpleCollectionKeyFilter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, SimpleCollectionKeyFilter object) throws IOException {
         output.writeObject(object.collection);
      }

      @Override
      public SimpleCollectionKeyFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SimpleCollectionKeyFilter((Collection<? extends Object>)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.SIMPLE_COLLECTION_KEY_FILTER;
      }
   }
}
