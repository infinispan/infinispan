package org.infinispan.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.marshall.core.MarshalledValue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Set;

/**
 * Filter based on accepting/rejecting the keys that are present in a supplied collection.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class CollectionKeyFilter<K> implements KeyFilter<K> {

   private final Collection<? extends K>  keys;
   private final boolean accept;

   public CollectionKeyFilter(Collection<? extends K> keys) {
      this(keys, false);
   }

   public CollectionKeyFilter(Collection<? extends K> keys, boolean accept) {
      this.keys = keys;
      this.accept = accept;
   }


   @Override
   public boolean accept(K key) {
      // This filter is provided as callback to cache stores, so it should be
      // able to take normal keys and verify them against seen keys which
      // might be MarshalledValues.
      boolean contains = keys.stream().filter(k -> k.equals(key) || MarshalledValue.unwrap(k).equals(key)).count() > 0;
      return accept ? contains : !contains;
   }

   public static class Externalizer extends AbstractExternalizer<CollectionKeyFilter> {
      @Override
      public Set<Class<? extends CollectionKeyFilter>> getTypeClasses() {
         return Util.<Class<? extends CollectionKeyFilter>>asSet(CollectionKeyFilter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, CollectionKeyFilter object) throws IOException {
         output.writeObject(object.keys);
         output.writeBoolean(object.accept);
      }

      @Override
      public CollectionKeyFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CollectionKeyFilter((Collection<? extends Object>)input.readObject(), input.readBoolean());
      }

      @Override
      public Integer getId() {
         return Ids.SIMPLE_COLLECTION_KEY_FILTER;
      }
   }
}
