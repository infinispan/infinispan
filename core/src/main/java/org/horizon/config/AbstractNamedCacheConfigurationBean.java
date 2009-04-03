package org.horizon.config;

import org.horizon.factories.ComponentRegistry;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;
import org.horizon.lifecycle.ComponentStatus;

/**
 * Adds named cache specific features to the {@link org.horizon.config.AbstractConfigurationBean}.
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
