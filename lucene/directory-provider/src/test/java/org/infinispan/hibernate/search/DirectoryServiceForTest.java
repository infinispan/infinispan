package org.infinispan.hibernate.search;

import org.hibernate.search.store.DirectoryProvider;
import org.hibernate.search.store.impl.FSDirectoryProvider;
import org.hibernate.search.store.spi.BaseDirectoryProviderService;
import org.infinispan.hibernate.search.spi.InfinispanDirectoryProvider;


public class DirectoryServiceForTest extends BaseDirectoryProviderService {
   @Override
   public Class<? extends DirectoryProvider> getDefault() {
      return FSDirectoryProvider.class;
   }

   @Override
   public String toFullyQualifiedClassName(String shortcut) {
      return InfinispanDirectoryProvider.class.getName();
   }
}
