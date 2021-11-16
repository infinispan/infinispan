package org.infinispan.security.impl;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.PrincipalRoleMapperContext;

/**
 * PrincipalRoleMapperContextImpl.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthorizationMapperContextImpl implements PrincipalRoleMapperContext {

   private final EmbeddedCacheManager cacheManager;

   public AuthorizationMapperContextImpl(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

}
