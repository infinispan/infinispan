package org.infinispan.commons.jdkspecific;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.spi.URLStreamHandlerProvider;

import org.kohsuke.MetaInfServices;

/**
 * A {@link URLStreamHandlerProvider} which handles URLs with protocol "classpath".
 * It is automatically registered using the service loader pattern. It can be disabled
 * by setting the system property <pre>org.infinispan.urlstreamhandler.skip</pre>
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
@MetaInfServices
public class ClasspathURLStreamHandlerProvider extends URLStreamHandlerProvider {
   private final boolean skipProvider = Boolean.getBoolean("org.infinispan.urlstreamhandler.skip");

   @Override
   public URLStreamHandler createURLStreamHandler(String protocol) {
      if (!skipProvider && "classpath".equals(protocol)) {
         return new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL u) throws IOException {
               String path = u.getPath();
               ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
               URL resource = classLoader == null ? null : classLoader.getResource(path);
               if (resource == null) {
                  resource = ClassLoader.getSystemClassLoader().getResource(path);
               }
               if (resource != null) {
                  return resource.openConnection();
               } else {
                  throw new FileNotFoundException(u.toString());
               }
            }
         };
      }
      return null;
   }
}
