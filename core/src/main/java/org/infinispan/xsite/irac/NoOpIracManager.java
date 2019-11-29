package org.infinispan.xsite.irac;

import java.util.Collection;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Scope(Scopes.NAMED_CACHE)
public class NoOpIracManager implements IracManager {

   private static final NoOpIracManager INSTANCE = new NoOpIracManager();

   private NoOpIracManager() {
   }

   public static NoOpIracManager getInstance() {
      return INSTANCE;
   }

   @Override
   public void trackUpdatedKey(Object key, Object lockOwner) {
      //no-op
   }

   @Override
   public <K> void trackUpdatedKeys(Collection<K> keys, Object lockOwner) {
      //no-op
   }

   @Override
   public void trackClear() {
      //no-op
   }

   @Override
   public void cleanupKey(Object key, Object lockOwner) {
      //no-op
   }
}
