package org.horizon.factories;

import org.horizon.config.Configuration;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;

/**
 * A component factory for creating components scoped per-cache.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class AbstractNamedCacheComponentFactory extends AbstractComponentFactory {
   protected Configuration configuration;
   protected ComponentRegistry componentRegistry;

   @Inject
   private void injectGlobalDependencies(Configuration configuration, ComponentRegistry componentRegistry) {
      this.componentRegistry = componentRegistry;
      this.configuration = configuration;
   }
}
