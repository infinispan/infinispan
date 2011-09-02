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

   @Inject
   public void inject(ClassLoader loader, GlobalConfiguration globalCfg, ExternalizerTable extTable) {
      super.inject(extTable);
      this.marshaller = createMarshaller(globalCfg, loader);
      this.marshaller.inject(null, loader, null);
   }

   @Start(priority = 9) // Should start before Transport component
   public void start() {
      super.start();
   }

   @Stop(priority = 11) // Stop after transport to avoid send/receive and marshaller not being ready
   public void stop() {
      super.stop();
   }

}
