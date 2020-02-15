package org.infinispan.server.integration;

import org.jboss.arquillian.core.spi.LoadableExtension;

public class WildflyResoucesProcessorExtension implements LoadableExtension {
   @Override
   public void register(ExtensionBuilder extensionBuilder) {
      extensionBuilder.observer(WildflyResoucesProcessorExecuter.class);
   }
}
