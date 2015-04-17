package org.infinispan.hibernate.search.spi;

import org.hibernate.search.engine.service.spi.Service;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * @author Hardy Ferentschik
 */
public interface CacheManagerService extends Service {

   EmbeddedCacheManager getEmbeddedCacheManager();

}
