package org.infinispan.search.mapper.search.loading.context.impl;

import java.util.Optional;
import java.util.Set;

import org.hibernate.search.mapper.pojo.loading.spi.PojoLoadingTypeContext;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingContext;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingContextBuilder;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionLoadingStrategy;
import org.hibernate.search.mapper.pojo.model.spi.PojoRuntimeIntrospector;
import org.infinispan.search.mapper.model.impl.InfinispanRuntimeIntrospector;

/**
 * @author Fabio Massimo Ercoli
 */
public final class InfinispanLoadingContext<E> implements PojoSelectionLoadingContext  {

   private final PojoSelectionEntityLoader<E> entityLoader;

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

   @Override
   public <T> PojoSelectionLoadingStrategy<? super T> loadingStrategy(PojoLoadingTypeContext<T> type) {
      InfinispanSelectionLoadingStrategy<E> loadingStrategy = new InfinispanSelectionLoadingStrategy<>(entityLoader);
      return (PojoSelectionLoadingStrategy<? super T>) loadingStrategy;
   }

   @Override
   public <T> Optional<PojoSelectionLoadingStrategy<? super T>> loadingStrategyOptional(PojoLoadingTypeContext<T> type) {
      return Optional.of( loadingStrategy( type ) );
   }

   public static final class Builder<E> implements PojoSelectionLoadingContextBuilder<Void> {
      private final PojoSelectionEntityLoader<E> entityLoader;

      public Builder(PojoSelectionEntityLoader<E> entityLoader) {
         this.entityLoader = entityLoader;
      }

      @Override
      public Void toAPI() {
         // loading options are not used by ISPN
         return null;
      }

      @Override
      public InfinispanLoadingContext build() {
         return new InfinispanLoadingContext(entityLoader);
      }
   }

   private class InfinispanSelectionLoadingStrategy<E> implements PojoSelectionLoadingStrategy<E> {

      private final PojoSelectionEntityLoader<E> entityLoader;

      private InfinispanSelectionLoadingStrategy(PojoSelectionEntityLoader<E> entityLoader) {
         this.entityLoader = entityLoader;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }
         InfinispanSelectionLoadingStrategy<?> that = (InfinispanSelectionLoadingStrategy<?>) o;
         return entityLoader.equals(that.entityLoader);
      }

      @Override
      public int hashCode() {
         return entityLoader.hashCode();
      }

      @Override
      public PojoSelectionEntityLoader<E> createLoader(Set<? extends PojoLoadingTypeContext<? extends E>> expectedTypes) {
         return entityLoader;
      }
   }
}
