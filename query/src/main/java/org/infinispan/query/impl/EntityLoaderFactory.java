package org.infinispan.query.impl;

import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingContext;
import org.infinispan.AdvancedCache;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.search.mapper.search.loading.context.impl.InfinispanLoadingContext;

public class EntityLoaderFactory<E> {

   private final EntityLoader<E> entityLoader;
   private final MetadataEntityLoader<E> metadataEntityLoader;

   public EntityLoaderFactory(AdvancedCache<?, E> cache, LocalQueryStatistics queryStatistics) {
      this.entityLoader = new EntityLoader<>(cache, queryStatistics);
      this.metadataEntityLoader = new MetadataEntityLoader<>(cache, queryStatistics);
   }

   public InfinispanLoadingContext.Builder<E> builder() {
      return new InfinispanLoadingContext.Builder<>(entityLoader, metadataEntityLoader);
   }

   public PojoSelectionLoadingContext create() {
      return builder().build();
   }
}
