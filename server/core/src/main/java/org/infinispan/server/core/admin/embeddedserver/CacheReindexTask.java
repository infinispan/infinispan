package org.infinispan.server.core.admin.embeddedserver;

import static org.infinispan.util.concurrent.CompletionStages.join;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
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
   private static final Set<String> PARAMETERS = Collections.singleton("name");

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
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters, EnumSet<CacheContainerAdmin.AdminFlag> adminFlags) {
      if(!adminFlags.isEmpty())
         throw new UnsupportedOperationException();

      String name = requireParameter(parameters, "name");
      Cache<Object, Object> cache = cacheManager.getCache(name);
      join(Search.getIndexer(cache).run());

      return null;
   }
}
