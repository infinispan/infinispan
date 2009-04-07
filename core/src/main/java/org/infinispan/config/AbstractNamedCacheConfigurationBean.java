package org.infinispan.config;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * Adds named cache specific features to the {@link org.infinispan.config.AbstractConfigurationBean}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class AbstractNamedCacheConfigurationBean extends AbstractConfigurationBean {

   protected ComponentRegistry cr;

   @Inject
   private void inject(ComponentRegistry cr) {
      this.cr = cr;
   }

   protected boolean hasComponentStarted() {
      return cr != null && cr.getStatus() != null && cr.getStatus() == ComponentStatus.RUNNING;
   }
}
