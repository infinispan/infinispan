package org.infinispan.persistence.support;

import java.util.function.ToIntFunction;

import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.persistence.spi.SegmentedAdvancedLoadWriteStore;

/**
 * Abstract segment loader writer that implements all the single key non segmented methods by invoking the segmented
 * equivalent by passing in the segment returned from invoking {@link #getKeyMapper()}. These methods are also all
 * declared final as to make sure the end user does not implement the incorrect method. All other methods must be
 * implemented as normal.
 * @author wburns
 * @since 9.4
 */
public abstract class AbstractSegmentedAdvancedLoadWriteStore<K, V> implements SegmentedAdvancedLoadWriteStore<K, V> {
   protected abstract ToIntFunction<Object> getKeyMapper();

   @Override
   public final MarshalledEntry<K, V> load(Object key) {
      return load(getKeyMapper().applyAsInt(key), key);
   }

   @Override
   public final boolean contains(Object key) {
      return contains(getKeyMapper().applyAsInt(key), key);
   }

   @Override
   public final void write(MarshalledEntry<? extends K, ? extends V> entry) {
      write(getKeyMapper().applyAsInt(entry.getKey()), entry);
   }

   @Override
   public final boolean delete(Object key) {
      return delete(getKeyMapper().applyAsInt(key), key);
   }
}
