package org.infinispan.query;

import org.infinispan.Cache;
import org.infinispan.query.impl.SearchManagerImpl;

/**
 * Helper class to get a SearchManager out of an indexing enabled cache.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class Search {
   
   public static SearchManager getSearchManager(Cache<?, ?> cache) {
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      return new SearchManagerImpl(cache.getAdvancedCache());
   }

}
