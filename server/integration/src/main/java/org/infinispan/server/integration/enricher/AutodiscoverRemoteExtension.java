package org.infinispan.server.integration.enricher;

import org.jboss.arquillian.container.test.spi.RemoteLoadableExtension;

public class AutodiscoverRemoteExtension implements RemoteLoadableExtension {

   @Override
   public void register(ExtensionBuilder extensionBuilder) {
      extensionBuilder.observer(Enricher.class);
   }
}
