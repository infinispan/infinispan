package org.infinispan.query.impl;

import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingContext;
import org.infinispan.AdvancedCache;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.mapper.search.loading.context.impl.InfinispanLoadingContext;

public class EntityLoaderFactory<E> {

   private final EntityLoader<E> entityLoader;
   private final MetadataEntityLoader<E> metadataEntityLoader;

   @SuppressWarnings("unchecked")
   public EntityLoaderFactory(AdvancedCache<?, ?> cache, LocalQueryStatistics queryStatistics) {
      this.entityLoader = new EntityLoader<>((AdvancedCache<Object, Object>) cache, queryStatistics);
      this.metadataEntityLoader = new MetadataEntityLoader<>((AdvancedCache<Object, Object>) cache, queryStatistics);
   }

   public InfinispanLoadingContext.Builder<E> builder() {
      return new InfinispanLoadingContext.Builder<>(entityLoader, metadataEntityLoader);
   }

   public PojoSelectionLoadingContext create() {
      return builder().build();
   }
}
