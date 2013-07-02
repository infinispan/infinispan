package org.infinispan.loaders.decorators;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ObjectInput;
import java.util.List;

/**
 * A decorator that makes the underlying store a {@link org.infinispan.loaders.CacheLoader}, i.e., suppressing all write
 * methods.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ReadOnlyStore extends AbstractDelegatingStore {
   private static final Log log = LogFactory.getLog(ReadOnlyStore.class);

   public ReadOnlyStore(CacheStore delegate) {
      super(delegate);
   }

   @Override
   public void store(InternalCacheEntry ed) {
      log.trace("Ignoring store invocation"); 
   }

   @Override
   public void fromStream(ObjectInput inputStream) {
      log.trace("Ignoring writing contents of stream to store");
   }

   @Override
   public void clear() {
      log.trace("Ignoring clear invocation");
   }

   @Override
   public boolean remove(Object key) {
      log.trace("Ignoring removal of key");
      return false;  // no-op
   }

   @Override
   public void purgeExpired() {
      log.trace("Ignoring purge expired invocation");
   }

   @Override
   public void commit(GlobalTransaction tx) {
      log.trace("Ignoring transactional commit call");
   }

   @Override
   public void rollback(GlobalTransaction tx) {
      log.trace("Ignoring transactional rollback call");
   }

   @Override
   public void prepare(List<? extends Modification> list, GlobalTransaction tx, boolean isOnePhase) {
      log.trace("Ignoring transactional prepare call");
   }
}
