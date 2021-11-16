package org.infinispan.security;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * @since 14.0
 **/
public interface AuthorizationMapperContext {
   EmbeddedCacheManager getCacheManager();
}
