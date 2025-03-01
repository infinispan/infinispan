package org.infinispan.server.core.admin.embeddedserver;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Admin operation to remove a cache
 * Parameters:
 * <ul>
 *    <li><b>name</b> the name of the cache to remove</li>
 *    <li><b>flags</b> </li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class CacheRemoveTask extends AdminServerTask<Void> {
   private static final Set<String> PARAMETERS = Collections.singleton("name");

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
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters, EnumSet<CacheContainerAdmin.AdminFlag> adminFlags) {
      String name = requireParameter(parameters,"name");
      cacheManager.administration().withFlags(adminFlags).removeCache(name);
      return null;
   }
}
