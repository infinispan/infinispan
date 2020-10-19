package org.infinispan.server.core.admin.embeddedserver;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 *  * Admin operation to create a template
 *  * Parameters:
 *  * <ul>
 *  *    <li><strong>name</strong> the name of the template to create</li>
 *  *    <li><strong>configuration</strong> the XML configuration to use</li>
 *  *    <li><strong>flags</strong> any flags, e.g. PERMANENT</li>
 *  * </ul>
 *
 * @author Ryan Emerson
 * @since 12.0
 */
public class TemplateCreateTask extends AdminServerTask<Void> {
   private static final Set<String> PARAMETERS;

   static {
      Set<String> params = new HashSet<>(2);
      params.add("name");
      params.add("configuration");
      PARAMETERS = Collections.unmodifiableSet(params);
   }

   @Override
   public String getTaskContextName() {
      return "template";
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
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters,
                          EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      String name = requireParameter(parameters, "name");
      String configuration = requireParameter(parameters, "configuration");
      Configuration config = getConfiguration(name, configuration);
      if (!cacheManager.getCacheManagerConfiguration().isClustered() && config.clustering().cacheMode().isClustered()) {
         throw log.cannotCreateClusteredCache();
      }
      cacheManager.administration().withFlags(flags).createTemplate(name, config);
      return null;
   }

   protected Configuration getConfiguration(String name, String configuration) {
      ParserRegistry parserRegistry = new ParserRegistry();
      ConfigurationBuilderHolder builderHolder = parserRegistry.parse(configuration);
      if (!builderHolder.getNamedConfigurationBuilders().containsKey(name)) {
         throw log.missingCacheConfiguration(name, configuration);
      }
      return builderHolder.getNamedConfigurationBuilders().get(name).build();
   }
}
