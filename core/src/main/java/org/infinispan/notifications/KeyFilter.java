package org.infinispan.notifications;

/**
 * A filter for keys.
 *
 * @author Manik Surtani
 * @since 6.0
 */
public interface KeyFilter {

   /**
    * @param key key to test
    * @return true if the given key is accepted by this filter.
    */
   boolean accept(Object key);
}
