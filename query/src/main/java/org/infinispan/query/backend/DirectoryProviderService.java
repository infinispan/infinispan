package org.infinispan.query.backend;

import org.hibernate.search.store.BaseDirectoryProviderService;
import org.hibernate.search.store.DirectoryProvider;
import org.hibernate.search.store.impl.FSDirectoryProvider;
import org.infinispan.hibernate.search.spi.InfinispanDirectoryProvider;

/**
 * Customizes initialization of the DirectoryProviders in the Hibernate Search engine. In particular, sets the
 * InfinispanDirectory
 *
 * @author gustavonalle
 * @since 7.2
 */
public class DirectoryProviderService extends BaseDirectoryProviderService {
   @Override
   public Class<? extends DirectoryProvider> getDefault() {
      return FSDirectoryProvider.class;
   }

   @Override
   public String toFullyQualifiedClassName(String name) {
      String maybeShortCut = name.toLowerCase();
      if (maybeShortCut.equals("infinispan")) {
         return InfinispanDirectoryProvider.class.getName();
      }
      if (defaultProviderClasses.containsKey(maybeShortCut)) {
         return defaultProviderClasses.get(maybeShortCut);
      }
      return name;
   }
}
