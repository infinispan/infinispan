package org.infinispan.query.dsl.embedded.impl;

import org.hibernate.search.engine.metadata.impl.EmbeddedTypeMetadata;
import org.hibernate.search.engine.metadata.impl.TypeMetadata;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.objectfilter.impl.syntax.BooleShannonExpansion;

import java.util.List;

/**
 * Tests if a field is indexed by examining the Hibernate Search metadata.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class HibernateSearchIndexedFieldProvider implements BooleShannonExpansion.IndexedFieldProvider {

   private final SearchIntegrator searchFactory;

   private final Class<?> entityClass;

   public HibernateSearchIndexedFieldProvider(SearchIntegrator searchFactory, Class<?> entityClass) {
      this.searchFactory = searchFactory;
      this.entityClass = entityClass;
   }

   @Override
   public boolean isIndexed(List<String> propertyPath) {
      EntityIndexBinding entityIndexBinding = searchFactory.getIndexBinding(entityClass);
      if (entityIndexBinding == null) {
         return false;
      }

      TypeMetadata typeMetadata = entityIndexBinding.getDocumentBuilder().getMetadata();
      for (int i = 0; i < propertyPath.size() - 1; i++) {
         typeMetadata = getEmbeddedTypeMetadata(typeMetadata, propertyPath.get(i));
         if (typeMetadata == null) {
            return false;
         }
      }
      String last = propertyPath.get(propertyPath.size() - 1);
      return typeMetadata.getPropertyMetadataForProperty(last) != null || getEmbeddedTypeMetadata(typeMetadata, last) != null;
   }

   private EmbeddedTypeMetadata getEmbeddedTypeMetadata(TypeMetadata typeMetadata, String name) {
      for (EmbeddedTypeMetadata embeddedTypeMetadata : typeMetadata.getEmbeddedTypeMetadata()) {
         if (embeddedTypeMetadata.getEmbeddedFieldName().equals(name)) {
            return embeddedTypeMetadata;
         }
      }
      return null;
   }
}
