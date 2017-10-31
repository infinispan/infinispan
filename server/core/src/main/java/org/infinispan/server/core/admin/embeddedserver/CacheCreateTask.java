package org.infinispan.server.core.admin.embeddedserver;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Admin operation to create a cache Parameters: <ul> <li><strong>name</strong> the name of the cache to remove</li>
 * <li><strong>flags</strong> </li> </ul>
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class CacheCreateTask extends AdminServerTask<Void> {
   private static Set<String> PARAMETERS;

   static {
      PARAMETERS = new HashSet<>();
      PARAMETERS.add("name");
      PARAMETERS.add("template");
   }

   @Override
   public String getTaskContextName() {
      return "cache";
   }

   @Override
   public String getTaskOperationName() {
      return "create";
   }

   @Override
   public Set<String> getParameters() {
      return PARAMETERS;
   }

   @Override
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, String> parameters, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      String name = requireParameter(parameters, "name");
      String template = getParameter(parameters, "template");
      cacheManager.administration().withFlags(flags).createCache(name, template);

      return null;
   }
}
