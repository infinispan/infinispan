package org.infinispan.server.core.admin.embeddedserver;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Administrative operation to update the index schema for a cache with the following parameters:
 * Parameters:
 * <ul>
 *    <li><strong>name</strong> specifies the cache for which its index schema will be updated.</li>
 *    <li><strong>flags</strong> unused</li>
 * </ul>
 *
 * @author Fabio Massimo Ercoli
 * @since 14.0
 */
public class CacheUpdateIndexSchemaTask extends AdminServerTask<Void> {
   private static final Set<String> PARAMETERS = Collections.singleton("name");

   @Override
   public String getTaskContextName() {
      return "cache";
   }

   @Override
   public String getTaskOperationName() {
      return "updateindexschema";
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

      SearchMapping searchMapping = ComponentRegistryUtils.getSearchMapping(cache);
      searchMapping.restart();

      return null;
   }
}
