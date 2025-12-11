package org.infinispan.commons.util;

import java.security.Provider;
import java.security.Security;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class SecurityProviders {
   private static final ConcurrentHashMap<ClassLoader, List<Provider>> PER_CLASSLOADER_PROVIDERS = new ConcurrentHashMap<>(2);

   private SecurityProviders() {
   }

   public static Provider findProvider(String providerName, String serviceType, String algorithm) {
      Provider[] providers = discoverSecurityProviders(Thread.currentThread().getContextClassLoader());
      for (Provider provider : providers) {
         if (providerName == null || providerName.equals(provider.getName())) {
            Provider.Service providerService = provider.getService(serviceType, algorithm);
            if (providerService != null) {
               return provider;
            }
         }
      }
      return null;
   }

   public static Provider[] discoverSecurityProviders(ClassLoader classLoader) {
      return PER_CLASSLOADER_PROVIDERS.computeIfAbsent(classLoader, cl -> {
               // We need to keep them sorted by insertion order, since we want system providers first
               Map<Class<? extends Provider>, Provider> providers = new LinkedHashMap<>();
               for (Provider provider : Security.getProviders()) {
                  providers.put(provider.getClass(), provider);
               }
               for (Provider provider : ServiceFinder.load(Provider.class, classLoader)) {
                  providers.putIfAbsent(provider.getClass(), provider);
               }
               return List.copyOf(providers.values());
            }
      ).toArray(new Provider[0]);
   }

}
