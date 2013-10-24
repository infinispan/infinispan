package org.infinispan.commons.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * ServiceFinder is a {@link java.util.ServiceLoader} replacement which understands multiple
 * classpaths
 * 
 * @author Tristan Tarrant
 * @since 6.0
 */
public class ServiceFinder {

   public static <S> Collection<Class<S>> load(Class<S> service, ClassLoader... loaders) {
      Set<Class<S>> services = new LinkedHashSet<Class<S>>();
      for (ClassLoader loader : loaders) {
         if (loader == null)
            continue;
         try {
            for (Enumeration<URL> resources = loader.getResources("META-INF/services/" + service.getName()); resources
                  .hasMoreElements();) {
               URL resource = resources.nextElement();
               BufferedReader r = new BufferedReader(new InputStreamReader(resource.openStream()));
               for (String line = r.readLine(); line != null; line = r.readLine()) {
                  line = line.trim();
                  if (!line.startsWith("#")) {
                     Class<?> klass;
                     try {
                        klass = loader.loadClass(line);
                        if (!service.isAssignableFrom(klass)) {
                           throw new ClassCastException("Class " + line + " does not implement " + service.getName());
                        }
                        services.add((Class<S>) klass);
                     } catch (ClassNotFoundException e) {
                     }
                  }
               }
               r.close();
            }

         } catch (IOException e) {
            // Ignore
         }

      }

      return services;
   }
}
