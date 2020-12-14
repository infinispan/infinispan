package org.infinispan.commons.configuration.io;

import java.io.IOException;
import java.net.URL;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public interface ConfigurationResourceResolver {
   ConfigurationResourceResolver DEFAULT = new URLConfigurationResourceResolver(null);

   URL resolveResource(String href) throws IOException;
}
