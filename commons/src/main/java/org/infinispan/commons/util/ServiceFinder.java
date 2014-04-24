package org.infinispan.commons.util;

import java.util.LinkedHashSet;
import java.util.ServiceLoader;
import java.util.Set;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
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

   private static final Log LOG = LogFactory.getLog(ServiceFinder.class);
   
   public static <T> Set<T> load(Class<T> contract, ClassLoader... loaders) {
      Set<T> services = new LinkedHashSet<T>();
      
      if (loaders.length == 0) {
         try {
            ServiceLoader<T> loadedServices = ServiceLoader.load(contract);
            addServices( loadedServices, services );
         } catch (Exception e) {
            // Ignore
         }
      }
      else {
         for (ClassLoader loader : loaders) {
            if (loader == null)
               continue;
            try {
               ServiceLoader<T> loadedServices = ServiceLoader.load(contract, loader);
               addServices( loadedServices, services );
            } catch (Exception e) {
               // Ignore
            }
         }
      }
      
      addOsgiServices( contract, services );
      
      if (services.isEmpty()) {
         LOG.debugf("No service impls found: %s", contract.getSimpleName());
      }

      return services;
   }
   
   private static <T> void addServices(ServiceLoader<T> loadedServices, Set<T> services) {
      if (loadedServices.iterator().hasNext()) {
         for (T loadedService : loadedServices) {
            LOG.debugf("Loading service impl: %s", loadedService.getClass().getSimpleName());
            services.add(loadedService);
         }
      }
   }

   private static <T> void addOsgiServices(Class<T> contract, Set<T> services) {
      if (!Util.isOSGiContext()) {
          return;
      }
      ClassLoader loader = ServiceFinder.class.getClassLoader();
      if ((loader != null) && (loader instanceof org.osgi.framework.BundleReference)) {
         final BundleContext bundleContext = ((BundleReference) loader).getBundle().getBundleContext();
         final ServiceTracker<T, T> serviceTracker = new ServiceTracker<T, T>(bundleContext, contract.getName(),
               null);
         serviceTracker.open();
         try {
            final Object[] osgiServices = serviceTracker.getServices();
            if (osgiServices != null) {
               for (Object osgiService : osgiServices) {
                  services.add((T) osgiService);
               }
            }
         } catch (Exception e) {
            // ignore
         }
      }
   }
}
