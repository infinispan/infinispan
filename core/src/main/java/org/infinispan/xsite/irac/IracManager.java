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
public interface IracManager {

   void trackUpdatedKey(Object key, Object lockOwner);

   <K> void  trackUpdatedKeys(Collection<K> keys, Object lockOwner);

   void trackClear();

   void cleanupKey(Object key, Object lockOwner);

}
