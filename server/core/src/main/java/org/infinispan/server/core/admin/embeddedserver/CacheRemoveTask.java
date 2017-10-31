package org.infinispan.server.core.admin.embeddedserver;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Admin operation to remove a cache
 * Parameters:
 * <ul>
 *    <li><strong>name</strong> the name of the cache to remove</li>
 *    <li><strong>flags</strong> </li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class CacheRemoveTask extends AdminServerTask<Void> {
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
      return "remove";
   }

   @Override
   public Set<String> getParameters() {
      return PARAMETERS;
   }

   @Override
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, String> parameters, EnumSet<CacheContainerAdmin.AdminFlag> adminFlags) {
      String name = requireParameter(parameters,"name");
      cacheManager.administration().withFlags(adminFlags).removeCache(name);
      return null;
   }
}
