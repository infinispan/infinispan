package org.infinispan.marshall;

import org.infinispan.commons.marshall.AbstractDelegatingMarshaller;
import org.infinispan.config.Configuration;
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
   public void inject(Configuration cfg, InvocationContextContainer icc, ExternalizerTable extTable) {
      ((VersionAwareMarshaller) this.marshaller).inject(cfg, null, icc, extTable);
   }

   @Stop(priority = 11) // Stop after RPCManager to avoid send/receive and marshaller not being ready
   public void stop() {
      super.stop();
   }

}
