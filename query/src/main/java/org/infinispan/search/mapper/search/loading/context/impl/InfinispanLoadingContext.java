package org.infinispan.search.mapper.search.loading.context.impl;

import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingContext;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingContextBuilder;
import org.hibernate.search.mapper.pojo.model.spi.PojoRuntimeIntrospector;
import org.infinispan.search.mapper.model.impl.InfinispanRuntimeIntrospector;


/**
 * @author Fabio Massimo Ercoli
 */
public final class InfinispanLoadingContext<E> implements PojoSelectionLoadingContext  {

   public final PojoSelectionEntityLoader<E> entityLoader;

   public InfinispanLoadingContext(PojoSelectionEntityLoader<E> entityLoader) {
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
      private final PojoSelectionEntityLoader<E> entityLoader;
      private final PojoSelectionEntityLoader<E> metadataEntityLoader;

      boolean withMetadata = false;

      public Builder(PojoSelectionEntityLoader<E> entityLoader, PojoSelectionEntityLoader<E> metadataEntityLoader) {
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
