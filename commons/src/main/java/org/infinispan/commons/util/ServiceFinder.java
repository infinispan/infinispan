package org.infinispan.commons.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Set;

import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleReference;
import org.osgi.util.tracker.ServiceTracker;

/**
 * ServiceFinder is a {@link java.util.ServiceLoader} replacement which understands multiple
 * classpaths
 * 
 * @author Tristan Tarrant
 * @author Brett Meyer
 * @since 6.0
 */
public class ServiceFinder {

   public static <T> Collection<Class<T>> load(Class<T> contract, ClassLoader... loaders) {
      Set<Class<T>> services = new LinkedHashSet<Class<T>>();
      for (ClassLoader loader : loaders) {
         if (loader == null)
            continue;
         try {
        	final Enumeration<URL> resources = loader.getResources("META-INF/services/" + contract.getName());
            while ( resources.hasMoreElements() ) {
               URL resource = resources.nextElement();
               BufferedReader r = new BufferedReader(new InputStreamReader(resource.openStream()));
               for (String line = r.readLine(); line != null; line = r.readLine()) {
                  line = line.trim();
                  if (!line.startsWith("#")) {
                     Class<?> klass;
                     try {
                        klass = loader.loadClass(line);
                        if (!contract.isAssignableFrom(klass)) {
                           throw new ClassCastException("Class " + line + " does not implement " + contract.getName());
                        }
                        services.add((Class<T>) klass);
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
      
      addOsgiServices( contract, services, loaders );

      return services;
   }

   private static <T> void addOsgiServices(Class<T> contract, Set<Class<T>> services, ClassLoader... loaders) {
      for (ClassLoader loader : loaders) {
         if (loader instanceof BundleReference) {
            final BundleContext bundleContext = ((BundleReference) loader).getBundle().getBundleContext();
            final ServiceTracker<T, T> serviceTracker = new ServiceTracker<T, T>(bundleContext, contract.getName(),
                  null);
            serviceTracker.open();
            try {
               final Object[] osgiServices = serviceTracker.getServices();
               if (osgiServices != null) {
                  for (Object osgiService : osgiServices) {
                     services.add((Class<T>) osgiService.getClass());
                  }
               }
            } catch (Exception e) {
               // ignore
            }
         }
      }
   }
}
