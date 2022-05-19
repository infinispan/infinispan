package org.infinispan.commons.jdkspecific;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.spi.URLStreamHandlerProvider;

import org.kohsuke.MetaInfServices;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
@MetaInfServices
public class ClasspathURLStreamHandlerProvider extends URLStreamHandlerProvider {

   @Override
   public URLStreamHandler createURLStreamHandler(String protocol) {
      if ("classpath".equals(protocol)) {
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
                  return null;
               }
            }
         };
      }
      return null;
   }
}
