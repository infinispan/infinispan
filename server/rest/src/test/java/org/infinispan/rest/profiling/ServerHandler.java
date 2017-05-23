package org.infinispan.rest.profiling;

import org.infinispan.manager.EmbeddedCacheManager;

public interface ServerHandler {
   void start(EmbeddedCacheManager cacheManager);
   void stop();
}
