package org.infinispan.query.impl.massindex;

/**
 * Remove all indexes associated with a cache.
 *
 * @author gustavonalle
 * @since 7.0
 */
public interface IndexPurger {

   public void purge();

}
