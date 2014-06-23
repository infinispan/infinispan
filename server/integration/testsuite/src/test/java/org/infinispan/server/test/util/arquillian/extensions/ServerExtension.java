package org.infinispan.server.test.util.arquillian.extensions;

import org.jboss.arquillian.container.spi.ServerKillProcessor;
import org.jboss.arquillian.core.spi.LoadableExtension;

/**
 * ServerExtension
 *
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * @version $Revision: $
 */
public class ServerExtension implements LoadableExtension
{
    @Override
    public void register(ExtensionBuilder builder)
    {
        builder.service(ServerKillProcessor.class, InfinispanServerKillProcessor.class);
    }

}

