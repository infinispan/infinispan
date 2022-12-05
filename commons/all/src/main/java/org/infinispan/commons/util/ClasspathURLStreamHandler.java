package org.infinispan.commons.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

/**
 * A {@link URLStreamHandlerFactory} which can handle <tt>classpath:</tt> URI schemes. It will attempt to load resources
 * from the thread's context classloader (if it exists) and then fallback to the system classloader. The factory must be
 * registered as the URL stream handler factory using the {@link #register()} method.
 * <p>
 * On Java 9+, this class is available as a URLStreamHandlerProvider service loader implementation which, if present in
 * the boot classpath, will be automatically registered and used by the {@link URL} class.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class ClasspathURLStreamHandler implements URLStreamHandlerFactory {

   private static final URLStreamHandlerFactory INSTANCE = new ClasspathURLStreamHandler();

   /**
    * Registers this URL handler as the JVM-wide URL stream handler. It can only be invoked once in the lifecycle of an
    * application. Refer to the {@link URL} documentation for restrictions and alternative methods of registration.
    */
   public static void register() {
      URL.setURLStreamHandlerFactory(INSTANCE);
   }

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
                  throw new FileNotFoundException(u.toString());
               }
            }
         };
      }
      return null;
   }
}
