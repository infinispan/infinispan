package org.infinispan.jcache.util;

import javax.cache.spi.CachingProvider;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
public interface JCacheRunnable {

   /**
    * Run a task with a given {@link CachingProvider}.
    *
    * @param provider Caching provider to run the tasks against
    */
   void run(CachingProvider provider);

}
