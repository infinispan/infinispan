package org.infinispan.commons.configuration.io;

import java.io.IOException;
import java.net.URL;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class URLConfigurationResourceResolver implements ConfigurationResourceResolver {
   private final URL context;

   public URLConfigurationResourceResolver(URL context) {
      this.context = context;
   }

   @Override
   public URL resolveResource(String href) throws IOException {
      return new URL(context, href);
   }

   @Override
   public URL getContext() {
      return context;
   }
}
