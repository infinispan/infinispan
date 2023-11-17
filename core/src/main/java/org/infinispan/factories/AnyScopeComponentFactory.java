package org.infinispan.factories;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@Scope(Scopes.NONE)
public abstract class AnyScopeComponentFactory implements ComponentFactory {
   protected static final Log log = LogFactory.getLog(AbstractComponentFactory.class);
   @Inject protected GlobalComponentRegistry globalComponentRegistry;
   @Inject protected GlobalConfiguration globalConfiguration;
}
