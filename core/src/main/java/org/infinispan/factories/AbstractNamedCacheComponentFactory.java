package org.infinispan.factories;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * A component factory for creating components scoped per-cache.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class AbstractNamedCacheComponentFactory extends AbstractComponentFactory {
	protected GlobalConfiguration globalConfiguration;
   protected Configuration configuration;
   protected ComponentRegistry componentRegistry;

   @Inject
   private void injectGlobalDependencies(GlobalConfiguration globalConfiguration, Configuration configuration,
		   ComponentRegistry componentRegistry) {
	   this.globalConfiguration = globalConfiguration;
      this.componentRegistry = componentRegistry;
      this.configuration = configuration;
   }
}
