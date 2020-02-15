package org.infinispan.server.integration;

import org.jboss.arquillian.core.spi.LoadableExtension;

public class LifecycleExtension implements LoadableExtension {
   @Override
   public void register(ExtensionBuilder builder) {
      builder.observer(LifecycleExecuter.class);
   }
}
