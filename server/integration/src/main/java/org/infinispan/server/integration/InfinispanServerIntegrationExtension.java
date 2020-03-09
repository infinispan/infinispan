package org.infinispan.server.integration;

import org.infinispan.server.integration.enricher.Enricher;
import org.jboss.arquillian.core.spi.LoadableExtension;

public class InfinispanServerIntegrationExtension implements LoadableExtension {

   @Override
   public void register(ExtensionBuilder extensionBuilder) {
      extensionBuilder.observer(InfinispanServerIntegrationProcessorExecuter.class);
      if (ArquillianServerType.NONE.equals(ArquillianServerType.current())) {
         extensionBuilder.observer(Enricher.class);
      }
   }
}
