package org.infinispan.server.core.admin.embeddedserver;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Admin operation to (re)-assign an alias to a cache
 * Parameters:
 * <ul>
 *    <li><strong>name</strong> the name of the cache to which the alias should be assigned</li>
 *    <li><strong>alias</strong> the name of the alias</li>
 *    <li><strong>flags</strong> unused</li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 15.1
 */
public class CacheAssignAliasTask extends AdminServerTask<Void> {
   private static final Set<String> PARAMETERS = Set.of("name", "alias");

   @Override
   public String getTaskContextName() {
      return "cache";
   }

   @Override
   public String getTaskOperationName() {
      return "assignAlias";
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
      String alias = requireParameter(parameters, "alias");
      cacheManager.administration().assignAlias(alias, name);

      return null;
   }
}
