package org.infinispan.server.core.admin.embeddedserver;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Administrative operation to update a specific configuration attribute for a given cache with the following parameters:
 * Parameters:
 * <ul>
 *    <li><strong>name</strong> specifies the cache for which its configuration attribute will be updated.</li>
 *    <li><strong>attribute</strong> the path of the attribute we want to change, e.g. `indexing.indexed-entities`.</li>
 *    <li><strong>value</strong> the new value to apply to the attribute, e.g. `org.infinispan.Developer org.infinispan.Engineer`</li>
 *    <li><strong>flags</strong> any flags, e.g. PERMANENT</li>
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

      Cache<Object, Object> cache = cacheManager.getCache(cacheName);
      Configuration config = cache.getCacheConfiguration();
      Attribute<?> attribute = config.findAttribute(attributeName);
      attribute.fromString(attributeValue);

      flags.add(CacheContainerAdmin.AdminFlag.UPDATE);

      // doing what is done on CacheGetOrCreateTask
      cacheManager.administration().withFlags(flags).getOrCreateCache(cacheName, config);

      return null;
   }
}
