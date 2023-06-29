package org.infinispan.commons.configuration.io;

import java.io.IOException;
import java.net.URL;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public interface ConfigurationResourceResolver {

   // ISPN-15004 Lazily resolve URLConfigurationResourceResolver instance to prevent circular dependency
   // Replaced by ConfigurationResourceResolvers.DEFAULT instead. Variable only retained for backwards-compatibility.
   @Deprecated
   ConfigurationResourceResolver DEFAULT = href -> new URLConfigurationResourceResolver(null).resolveResource(href);

   URL resolveResource(String href) throws IOException;

   default URL getContext() {
      return null;
   }
}
