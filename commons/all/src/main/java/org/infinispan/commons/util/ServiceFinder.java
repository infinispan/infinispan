package org.infinispan.commons.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

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
            LOG.warnf("Skipping service: %s", Util.unwrapExceptionMessage(e));
            LOG.debug("Skipping service impl", e);
         }
      }
   }
}
