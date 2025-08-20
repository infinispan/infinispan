package org.infinispan.query.mapper.search.loading.context.impl;

import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingContext;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingContextBuilder;
import org.hibernate.search.mapper.pojo.model.spi.PojoRuntimeIntrospector;
import org.infinispan.query.impl.EntityLoaded;
import org.infinispan.query.mapper.model.impl.InfinispanRuntimeIntrospector;


/**
 * @author Fabio Massimo Ercoli
 */
public final class InfinispanLoadingContext<E> implements PojoSelectionLoadingContext  {

   public final PojoSelectionEntityLoader<EntityLoaded<E>> entityLoader;

   public InfinispanLoadingContext(PojoSelectionEntityLoader<EntityLoaded<E>> entityLoader) {
      this.entityLoader = entityLoader;
   }

   @Override
   public void checkOpen() {
      // Nothing to do: we're always "open"
   }

   @Override
   public PojoRuntimeIntrospector runtimeIntrospector() {
      return new InfinispanRuntimeIntrospector();
   }

   public static final class Builder<E> implements PojoSelectionLoadingContextBuilder<InfinispanSelectionLoadingOptionsStep>, InfinispanSelectionLoadingOptionsStep {
      private final PojoSelectionEntityLoader<EntityLoaded<E>> entityLoader;
      private final PojoSelectionEntityLoader<EntityLoaded<E>> metadataEntityLoader;

      boolean withMetadata = false;

      public Builder(PojoSelectionEntityLoader<EntityLoaded<E>> entityLoader, PojoSelectionEntityLoader<EntityLoaded<E>> metadataEntityLoader) {
         this.entityLoader = entityLoader;
         this.metadataEntityLoader = metadataEntityLoader;
      }

      @Override
      public InfinispanSelectionLoadingOptionsStep toAPI() {
         return this;
      }

      @Override
      public void withMetadata(boolean value) {
         withMetadata = value;
      }

      @Override
      public InfinispanLoadingContext<E> build() {
         return (withMetadata) ?  new InfinispanLoadingContext<>(metadataEntityLoader) :
               new InfinispanLoadingContext<>(entityLoader);
      }
   }
}
