package org.infinispan.server.core.admin.embeddedserver;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
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
   private static final Set<String> PARAMETERS = Set.of("name", "indexed-entities");

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

      HashSet<String> otherIndexedEntities = null;
      String indexedEntities = getParameter(parameters, "indexed-entities");
      if (indexedEntities != null) {
         otherIndexedEntities = new HashSet<>(Arrays.asList(indexedEntities.split(",")));
      }

      SearchMapping searchMapping = ComponentRegistryUtils.getSearchMapping(cache);
      searchMapping.restart(otherIndexedEntities);

      return null;
   }
}
