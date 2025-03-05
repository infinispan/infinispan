package org.infinispan.server.core.admin.embeddedserver;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Administrative operation to update a specific configuration attribute for a given cache with the following parameters:
 * Parameters:
 * <ul>
 *    <li><b>name</b> specifies the cache for which its configuration attribute will be updated.</li>
 *    <li><b>attribute</b> the path of the attribute we want to change, e.g. `indexing.indexed-entities`.</li>
 *    <li><b>value</b> the new value to apply to the attribute, e.g. `org.infinispan.Developer org.infinispan.Engineer`</li>
 *    <li><b>flags</b> any flags, e.g. PERMANENT</li>
 * </ul>
 * <p>
 * Note: the attribute is supposed to be mutable in order to be mutated by this operation.
 *
 * @author Fabio Massimo Ercoli
 * @since 14.0.7
 */
public class CacheUpdateConfigurationAttributeTask extends AdminServerTask<Void> {
   private static final Set<String> PARAMETERS = Set.of("name", "attribute", "value");

   @Override
   public String getTaskContextName() {
      return "cache";
   }

   @Override
   public String getTaskOperationName() {
      return "updateConfigurationAttribute";
   }

   @Override
   public Set<String> getParameters() {
      return PARAMETERS;
   }

   @Override
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters,
                          EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      String cacheName = requireParameter(parameters, "name");
      String attributeName = requireParameter(parameters, "attribute");
      String attributeValue = requireParameter(parameters, "value");

      cacheManager.administration().withFlags(flags).updateConfigurationAttribute(cacheName, attributeName, attributeValue);

      return null;
   }
}
