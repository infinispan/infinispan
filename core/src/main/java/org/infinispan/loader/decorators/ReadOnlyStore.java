package org.infinispan.loader.decorators;

import org.infinispan.loader.CacheStore;
import org.infinispan.loader.modifications.Modification;
import org.infinispan.container.entries.InternalCacheEntry;

import javax.transaction.Transaction;
import java.io.ObjectInput;
import java.util.List;

/**
 * A decorator that makes the underlying store a {@link org.infinispan.loader.CacheLoader}, i.e., suppressing all write
 * methods.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ReadOnlyStore extends AbstractDelegatingStore {

   public ReadOnlyStore(CacheStore delegate) {
      super(delegate);
   }

   @Override
   public void store(InternalCacheEntry ed) {
      // no-op
   }

   @Override
   public void fromStream(ObjectInput inputStream) {
      // no-op
   }

   @Override
   public void clear() {
      // no-op
   }

   @Override
   public boolean remove(Object key) {
      return false;  // no-op
   }

   @Override
   public void purgeExpired() {
      // no-op
   }

   @Override
   public void commit(Transaction tx) {
      // no-op
   }

   @Override
   public void rollback(Transaction tx) {
      // no-op
   }

   @Override
   public void prepare(List<? extends Modification> list, Transaction tx, boolean isOnePhase) {
      // no-op
   }
}
