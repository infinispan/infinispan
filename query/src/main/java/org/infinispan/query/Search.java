package org.infinispan.query;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.query.impl.SearchManagerImpl;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;

/**
 * Helper class to get a SearchManager out of an indexing enabled cache.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class Search {

   public static SearchManager getSearchManager(Cache<?, ?> cache) {
      if (cache == null || cache.getAdvancedCache() == null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      ensureAccessPermissions(cache.getAdvancedCache());
      return new SearchManagerImpl(cache.getAdvancedCache());
   }

   private static void ensureAccessPermissions(final AdvancedCache<?, ?> cache) {
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }
   }
}
