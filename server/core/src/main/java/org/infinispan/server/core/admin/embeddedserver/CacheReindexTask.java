package org.infinispan.server.core.admin.embeddedserver;

import org.infinispan.Cache;
import org.infinispan.query.Search;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Admin operation to reindex a cache
 * Parameters:
 * <ul>
 *    <li><strong>name</strong> the name of the cache to reindex</li>
 *    <li><strong>flags</strong> unused</li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class CacheReindexTask extends AdminServerTask<Void> {

   @Override
   public String getTaskContextName() {
      return "cache";
   }

   @Override
   public String getTaskOperationName() {
      return "reindex";
   }

   @Override
   public Void call() throws Exception {
      if (isPersistent())
         throw new UnsupportedOperationException();

      String name = requireParameter("name");
      Cache<Object, Object> cache = cacheManager.getCache(name);
      Search.getSearchManager(cache).getMassIndexer().start();

      return null;
   }
}
