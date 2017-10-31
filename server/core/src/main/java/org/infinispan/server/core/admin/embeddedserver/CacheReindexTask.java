package org.infinispan.server.core.admin.embeddedserver;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
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
   private static Set<String> PARAMETERS;

   static {
      PARAMETERS = new HashSet<>();
      PARAMETERS.add("name");
   }

   @Override
   public String getTaskContextName() {
      return "cache";
   }

   @Override
   public String getTaskOperationName() {
      return "reindex";
   }

   @Override
   public Set<String> getParameters() {
      return PARAMETERS;
   }

   @Override
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, String> parameters, EnumSet<CacheContainerAdmin.AdminFlag> adminFlags) {
      if(!adminFlags.isEmpty())
         throw new UnsupportedOperationException();

      String name = requireParameter(parameters, "name");
      Cache<Object, Object> cache = cacheManager.getCache(name);
      Search.getSearchManager(cache).getMassIndexer().start();

      return null;
   }
}
