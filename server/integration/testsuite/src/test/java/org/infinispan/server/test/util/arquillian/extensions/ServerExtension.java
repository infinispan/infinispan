package org.infinispan.server.test.util.arquillian.extensions;

import org.jboss.arquillian.container.spi.ServerKillProcessor;
import org.jboss.arquillian.core.spi.LoadableExtension;
import org.kohsuke.MetaInfServices;

/**
 * ServerExtension
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * @version $Revision: $
 */
@MetaInfServices
@SuppressWarnings("unused")
public class ServerExtension implements LoadableExtension {

   @Override
   public void register(ExtensionBuilder builder) {
      builder.service(ServerKillProcessor.class, InfinispanServerKillProcessor.class);
   }
}
