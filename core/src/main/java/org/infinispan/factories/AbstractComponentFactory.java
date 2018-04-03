package org.infinispan.factories;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Factory that creates components used internally within Infinispan, and also wires dependencies into the components.
 * <p/>
 * The {@link InternalCacheFactory} is a special subclass of this, which bootstraps the construction of other
 * components. When this class is loaded, it maintains a static list of known default factories for known components,
 * which it then delegates to, when actually performing the construction.
 * <p/>
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see Inject
 * @see ComponentRegistry
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public abstract class AbstractComponentFactory extends AnyScopeComponentFactory {

}
