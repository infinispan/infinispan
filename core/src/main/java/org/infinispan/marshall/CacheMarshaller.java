package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.jboss.ExternalizerTable;

/**
 * A cache-scoped marshaller.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Scope(Scopes.NAMED_CACHE)
public class CacheMarshaller extends AbstractDelegatingMarshaller {

   public CacheMarshaller(VersionAwareMarshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Inject
   public void inject(Cache cache, Configuration cfg, InvocationContextContainer icc,
            ExternalizerTable extTable, GlobalConfiguration globalCfg) {
      ((VersionAwareMarshaller) this.marshaller).inject(
            cache, cfg, null, icc, extTable, globalCfg);
   }

   @Override
   @Stop(priority = 11) // Stop after RPCManager to avoid send/receive and marshaller not being ready
   public void stop() {
      super.stop();
   }

}
