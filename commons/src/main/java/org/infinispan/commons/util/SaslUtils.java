package org.infinispan.commons.util;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.ServiceLoader;
import java.util.Set;

import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslServerFactory;

/**
 * Utility methods for handling SASL authentication
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 * @author Tristan Tarrant
 */
public final class SaslUtils {

   /**
    * Returns a collection of all of the registered {@code SaslServerFactory}s where the order is based on the
    * order of the Provider registration and/or class path order.  Class path providers are listed before
    * global providers; in the event of a name conflict, the class path provider is preferred.
    *
    * @param classLoader the class loader to use
    * @param includeGlobal {@code true} to include globally registered providers, {@code false} to exclude them
    * @return the {@code Iterator} of {@code SaslServerFactory}s
    */
   public static Collection<SaslServerFactory> getSaslServerFactories(ClassLoader classLoader, boolean includeGlobal) {
      return getFactories(SaslServerFactory.class, classLoader, includeGlobal);
   }

   /**
    * Returns a collection of all of the registered {@code SaslServerFactory}s where the order is based on the
    * order of the Provider registration and/or class path order.
    *
    * @return the {@code Iterator} of {@code SaslServerFactory}s
    */
   public static Collection<SaslServerFactory> getSaslServerFactories() {
      return getFactories(SaslServerFactory.class, null, true);
   }

   /**
    * Returns a collection of all of the registered {@code SaslClientFactory}s where the order is based on the
    * order of the Provider registration and/or class path order.  Class path providers are listed before
    * global providers; in the event of a name conflict, the class path provider is preferred.
    *
    * @param classLoader the class loader to use
    * @param includeGlobal {@code true} to include globally registered providers, {@code false} to exclude them
    * @return the {@code Iterator} of {@code SaslClientFactory}s
    */
   public static Collection<SaslClientFactory> getSaslClientFactories(ClassLoader classLoader, boolean includeGlobal) {
      return getFactories(SaslClientFactory.class, classLoader, includeGlobal);
   }

   /**
    * Returns a collection of all of the registered {@code SaslClientFactory}s where the order is based on the
    * order of the Provider registration and/or class path order.
    *
    * @return the {@code Iterator} of {@code SaslClientFactory}s
    */
   public static Collection<SaslClientFactory> getSaslClientFactories() {
      return getFactories(SaslClientFactory.class, null, true);
   }

   private static <T> Collection<T> getFactories(Class<T> type, ClassLoader classLoader, boolean includeGlobal) {
      Set<T> factories = new LinkedHashSet<>();
      final ServiceLoader<T> loader = ServiceLoader.load(type, classLoader);
      for (T factory : loader) {
         factories.add(factory);
      }
      if (includeGlobal) {
         Provider[] providers = Security.getProviders();
         for (Provider currentProvider : providers) {
            for (Provider.Service service : currentProvider.getServices()) {
               if (type.getSimpleName().equals(service.getType())) {
                  try {
                     factories.add((T) service.newInstance(null));
                  } catch (NoSuchAlgorithmException e) {
                  }
               }
            }
         }
      }
      return factories;
   }
}
