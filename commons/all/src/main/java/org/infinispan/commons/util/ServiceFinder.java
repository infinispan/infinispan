package org.infinispan.commons.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.util.tracker.ServiceTracker;

/**
 * ServiceFinder is a {@link java.util.ServiceLoader} replacement which understands multiple classpaths.
 *
 * @author Tristan Tarrant
 * @author Brett Meyer
 * @since 6.0
 */
public class ServiceFinder {

   private static final Log LOG = LogFactory.getLog(ServiceFinder.class);

   public static <T> Collection<T> load(Class<T> contract, ClassLoader... loaders) {
      Map<String, T> services = new LinkedHashMap<>();

      if (loaders.length == 0) {
         try {
            ServiceLoader<T> loadedServices = ServiceLoader.load(contract);
            addServices(loadedServices, services);
         } catch (Exception e) {
            // Ignore
         }
      }
      else {
         for (ClassLoader loader : loaders) {
            if (loader == null)
               throw new NullPointerException();
            try {
               ServiceLoader<T> loadedServices = ServiceLoader.load(contract, loader);
               addServices(loadedServices, services);
            } catch (Exception e) {
               // Ignore
            }
         }
      }

      addOsgiServices( contract, services );

      if (services.isEmpty()) {
         LOG.debugf("No service impls found: %s", contract.getSimpleName());
      }
      return services.values();
   }

   private static <T> void addServices(ServiceLoader<T> loadedServices, Map<String, T> services) {
      Iterator<T> i = loadedServices.iterator();
      while (i.hasNext()) {
         try {
            T service = i.next();
            if (services.putIfAbsent(service.getClass().getName(), service) == null) {
               LOG.debugf("Loading service impl: %s", service.getClass().getName());
            } else {
               LOG.debugf("Ignoring already loaded service: %s", service.getClass().getName());
            }
         } catch (ServiceConfigurationError e) {
            LOG.debugf("Skipping service impl", e);
         }
      }
   }

   private static <T> void addOsgiServices(Class<T> contract, Map<String, T> services) {
      if (!Util.isOSGiContext()) {
          return;
      }
      final BundleContext bundleContext = FrameworkUtil.getBundle(ServiceFinder.class).getBundleContext();
      final ServiceTracker<T, T> serviceTracker = new ServiceTracker<T, T>(bundleContext, contract.getName(),
            null);
      serviceTracker.open();
      try {
         final Object[] osgiServices = serviceTracker.getServices();
         if (osgiServices != null) {
            for (Object osgiService : osgiServices) {
               if (services.putIfAbsent(osgiService.getClass().getName(), (T) osgiService) == null) {
                  LOG.debugf("Loading service impl: %s", osgiService.getClass().getSimpleName());
               } else {
                  LOG.debugf("Ignoring already loaded service: %s", osgiService.getClass().getSimpleName());
               }
            }
         }
      } catch (Exception e) {
         // ignore
      }
   }
}
