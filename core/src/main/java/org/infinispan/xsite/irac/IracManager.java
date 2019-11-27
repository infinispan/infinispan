package org.infinispan.xsite.irac;

import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface IracManager {

   void trackUpdatedKey(Object key, Object lockOwner);

   void trackUpdatedKeys(Collection<Object> keys, Object lockOwner);

}
