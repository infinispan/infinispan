package org.infinispan.marshall;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
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
   public void inject(Configuration cfg, InvocationContextContainer icc,
            ExternalizerTable extTable, GlobalConfiguration globalCfg) {
      ((VersionAwareMarshaller) this.marshaller).inject(
            cfg, icc, extTable, globalCfg);
   }

   @Override
   @Start(priority = 8) // Stop before RPCManager to avoid send/receive and marshaller not being ready
   public void start() {
      this.marshaller.start();
   }

   @Override
   @Stop(priority = 11) // Stop after RPCManager to avoid send/receive and marshaller not being ready
   public void stop() {
      super.stop();
   }

}
