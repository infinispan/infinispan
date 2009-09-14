package org.infinispan.config;

import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * Adds named cache specific features to the {@link org.infinispan.config.AbstractConfigurationBean}
 * .
 * 
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class AbstractNamedCacheConfigurationBean extends AbstractConfigurationBean {

   protected ComponentRegistry cr;

   @Inject
   public void inject(ComponentRegistry cr) {
      this.cr = cr;
   }

   protected boolean hasComponentStarted() {
      return cr != null && cr.getStatus() != null && cr.getStatus() == ComponentStatus.RUNNING;
   }

   @Override
   public AbstractNamedCacheConfigurationBean clone() throws CloneNotSupportedException {
      AbstractNamedCacheConfigurationBean dolly = (AbstractNamedCacheConfigurationBean) super
               .clone();
      if (cr != null)
         dolly.cr = (ComponentRegistry) cr.clone();
      return dolly;
   }

   class InjectComponentRegistryVisitor extends AbstractConfigurationBeanVisitor {

      public void defaultVisit(AbstractConfigurationBean c) {
         if (c instanceof AbstractNamedCacheConfigurationBean) {
            ((AbstractNamedCacheConfigurationBean) c).cr = cr;
         }
      }
   }
}
