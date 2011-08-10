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

import static org.infinispan.factories.KnownComponentNames.GLOBAL_MARSHALLER;

/**
 * A cache-scoped marshaller.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Scope(Scopes.NAMED_CACHE)
public class CacheMarshaller extends AbstractDelegatingMarshaller {

   @Inject
   public void inject(GlobalConfiguration globalCfg, Configuration cfg,
                      InvocationContextContainer icc, ExternalizerTable extTable) {
      super.inject(extTable);
      this.marshaller = createMarshaller(globalCfg, cfg.getClassLoader());
      this.marshaller.inject(cfg, null, icc);
   }

   @Start(priority = 7) // should start before RPCManager
   public void start() {
      super.start();
   }

   @Stop(priority = 11) // Stop after RPCManager to avoid send/receive and marshaller not being ready
   public void stop() {
      super.stop();
   }

}
