package org.infinispan.security;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * PrincipalRoleMapperContext.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public interface PrincipalRoleMapperContext {
   /**
    * Returns the {@link EmbeddedCacheManager} in which this role mapper is being instantiated
    */
   EmbeddedCacheManager getCacheManager();
}
