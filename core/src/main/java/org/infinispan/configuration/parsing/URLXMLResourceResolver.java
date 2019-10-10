package org.infinispan.configuration.parsing;

import java.io.IOException;
import java.net.URL;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class URLXMLResourceResolver implements XMLResourceResolver {

   private final URL context;

   public URLXMLResourceResolver(URL context) {
      this.context = context;
   }

   @Override
   public URL resolveResource(String href) throws IOException {
      return new URL(context, href);
   }
}
