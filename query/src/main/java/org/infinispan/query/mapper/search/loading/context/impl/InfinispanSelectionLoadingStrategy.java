package org.infinispan.query.mapper.search.loading.context.impl;

import org.hibernate.search.mapper.pojo.loading.spi.PojoLoadingTypeContext;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingContext;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingStrategy;

import java.util.Set;

public class InfinispanSelectionLoadingStrategy<E> implements PojoSelectionLoadingStrategy<E> {

   private static InfinispanSelectionLoadingStrategy INSTANCE = new InfinispanSelectionLoadingStrategy();

   private InfinispanSelectionLoadingStrategy() {
   }

   public static InfinispanSelectionLoadingStrategy instance() {
      return INSTANCE;
   }

   @Override
   public PojoSelectionEntityLoader<E> createEntityLoader(Set<? extends PojoLoadingTypeContext<? extends E>> set,
                                                          PojoSelectionLoadingContext context) {
      return ((InfinispanLoadingContext)context).entityLoader;
   }
}
