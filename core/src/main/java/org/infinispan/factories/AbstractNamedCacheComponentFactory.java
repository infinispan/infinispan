package org.infinispan.factories;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.BasicComponentRegistry;
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
   @Inject protected Configuration configuration;
   @Deprecated
   @Inject protected ComponentRegistry componentRegistry;
   @Inject protected BasicComponentRegistry basicComponentRegistry;
}
