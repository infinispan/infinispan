package org.infinispan.search.mapper.search.loading.context.impl;

import org.hibernate.search.engine.backend.common.spi.DocumentReferenceConverter;
import org.hibernate.search.engine.search.loading.context.spi.LoadingContext;
import org.hibernate.search.engine.search.loading.context.spi.LoadingContextBuilder;
import org.hibernate.search.engine.search.loading.spi.DefaultProjectionHitMapper;
import org.hibernate.search.engine.search.loading.spi.EntityLoader;
import org.hibernate.search.engine.search.loading.spi.ProjectionHitMapper;
import org.infinispan.search.mapper.common.EntityReference;

/**
 * @param <E> The entity type mapped to the index.
 *
 * @author Fabio Massimo Ercoli
 */
public final class InfinispanLoadingContext<E> implements LoadingContext<EntityReference, E> {

   private final DocumentReferenceConverter<EntityReference> documentReferenceConverter;
   private final EntityLoader<EntityReference, E> entityLoader;

   public InfinispanLoadingContext(DocumentReferenceConverter<EntityReference> documentReferenceConverter,
                                       EntityLoader<EntityReference, E> entityLoader) {
      this.documentReferenceConverter = documentReferenceConverter;
      this.entityLoader = entityLoader;
   }

   @Override
   public ProjectionHitMapper<EntityReference, E> createProjectionHitMapper() {
      return new DefaultProjectionHitMapper<>(documentReferenceConverter, entityLoader);
   }

   public static final class Builder<E> implements LoadingContextBuilder<EntityReference, E, Void> {
      private final DocumentReferenceConverter<EntityReference> documentReferenceConverter;
      private final EntityLoader<EntityReference, E> entityLoader;

      public Builder(DocumentReferenceConverter<EntityReference> documentReferenceConverter,
                     EntityLoader<EntityReference, E> entityLoader) {
         this.documentReferenceConverter = documentReferenceConverter;
         this.entityLoader = entityLoader;
      }

      @Override
      public Void toAPI() {
         // loading options are not used by ISPN
         return null;
      }

      @Override
      public LoadingContext<EntityReference, E> build() {
         return new InfinispanLoadingContext(documentReferenceConverter, entityLoader);
      }
   }
}
