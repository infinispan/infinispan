package org.infinispan.marshall;

import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.jboss.ExternalizerTable;

/**
 * A globally-scoped marshaller. This is needed so that the transport layer
 * can unmarshall requests even before it's known which cache's marshaller can
 * do the job.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Scope(Scopes.GLOBAL)
public class GlobalMarshaller extends AbstractDelegatingMarshaller {

   public GlobalMarshaller(VersionAwareMarshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Inject
   public void inject(ExternalizerTable extTable,
            GlobalConfiguration globalCfg) {
      ((VersionAwareMarshaller) this.marshaller).inject(
            null, null, extTable, globalCfg);
   }

   @Override
   @Start(priority = 8) // Should start after the externalizer table and before transport
   public void start() {
      this.marshaller.start();
   }

   @Override
   @Stop(priority = 11) // Stop after transport to avoid send/receive and marshaller not being ready
   public void stop() {
      super.stop();
   }

}
