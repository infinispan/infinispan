package org.infinispan.query.dsl.embedded.impl;

import java.text.ParseException;
import java.util.Date;
import java.util.Optional;
import java.util.Set;

import org.apache.lucene.document.DateTools;
import org.hibernate.search.engine.backend.metamodel.IndexDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldTypeDescriptor;
import org.hibernate.search.engine.backend.types.IndexFieldTraits;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.query.mapper.mapping.SearchIndexedEntity;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.objectfilter.ParsingException;
import org.infinispan.query.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.query.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.query.objectfilter.impl.syntax.parser.ReflectionPropertyHelper;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.CacheValuePropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.ScorePropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.VersionPropertyPath;
import org.infinispan.query.objectfilter.impl.util.StringHelper;

public final class HibernateSearchPropertyHelper extends ReflectionPropertyHelper {

   public static final String KEY = "__ISPN_Key";
   public static final String VALUE = CacheValuePropertyPath.VALUE_PROPERTY_NAME;
   public static final String VERSION = VersionPropertyPath.VERSION_PROPERTY_NAME;
   public static final String SCORE = ScorePropertyPath.SCORE_PROPERTY_NAME;

   private final SearchMapping searchMapping;

   public HibernateSearchPropertyHelper(SearchMapping searchMapping, EntityNameResolver<Class<?>> entityNameResolver) {
      super(entityNameResolver);
      this.searchMapping = searchMapping;
   }

   @Override
   public Object convertToPropertyType(Class<?> entityType, String[] propertyPath, String value) {
      IndexValueFieldDescriptor fieldDescriptor = getValueFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return super.convertToPropertyType(entityType, propertyPath, value);
      }

      Class<?> type = fieldDescriptor.type().dslArgumentClass();
      if (Date.class == type) {
         try {
            return DateTools.stringToDate(value);
         } catch (ParseException e) {
            throw new ParsingException(e.getMessage());
         }
      }

      return super.convertToPropertyType(entityType, propertyPath, value);
   }

   @Override
   public Class<?> getPrimitivePropertyType(Class<?> entityType, String[] propertyPath) {
      if (propertyPath.length == 1) {
         if (propertyPath[0].equals(VERSION)) {
            return EntryVersion.class;
         }
         if (propertyPath[0].equals(SCORE)) {
            return Float.class;
         }
      }

      IndexValueFieldDescriptor fieldDescriptor = getValueFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return super.getPrimitivePropertyType(entityType, propertyPath);
      }

      Class<?> type = fieldDescriptor.type().dslArgumentClass();
      if (type.isEnum()) {
         return type;
      }
      return primitives.get(type);
   }

   @Override
   public Class<?> getIndexedPropertyType(Class<?> entityType, String[] propertyPath) {
      IndexValueFieldDescriptor fieldDescriptor = getValueFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return null;
      }

      return fieldDescriptor.type().dslArgumentClass();
   }

   @Override
   public boolean isNestedIndexStructure(Class<?> entityType, String[] propertyPath) {
      IndexFieldDescriptor fieldDescriptor = getFieldDescriptor(entityType, propertyPath);
      return fieldDescriptor != null && fieldDescriptor.type().traits().contains(IndexFieldTraits.Predicates.NESTED);
   }

   @Override
   public boolean isRepeatedProperty(Class<?> entityType, String[] propertyPath) {
      IndexFieldDescriptor fieldDescriptor = getFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return super.isRepeatedProperty(entityType, propertyPath);
      }
      return fieldDescriptor.multiValuedInRoot();
   }

   @Override
   public boolean hasEmbeddedProperty(Class<?> entityType, String[] propertyPath) {
      IndexFieldDescriptor fieldDescriptor = getFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return super.hasEmbeddedProperty(entityType, propertyPath);
      }

      return fieldDescriptor.isObjectField();
   }

   @Override
   public boolean hasProperty(Class<?> entityType, String[] propertyPath) {
      IndexFieldDescriptor fieldDescriptor = getFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor != null) {
         return true;
      }

      if (propertyPath.length == 1 && (propertyPath[0].equals(KEY) || propertyPath[0].equals(VALUE) ||
            propertyPath[0].equals(VERSION) || propertyPath[0].equals(SCORE))) {
         return true;
      }

      return super.hasProperty(entityType, propertyPath);
   }

   @Override
   public IndexedFieldProvider<Class<?>> getIndexedFieldProvider() {
      return entityType -> {
         IndexDescriptor indexDescriptor = getIndexDescriptor(entityType);
         if (indexDescriptor == null) {
            return CLASS_NO_INDEXING;
         }

         return new SearchFieldIndexingMetadata(indexDescriptor);
      };
   }

   private IndexValueFieldDescriptor getValueFieldDescriptor(Class<?> entityType, String[] propertyPath) {
      IndexFieldDescriptor fieldDescriptor = getFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return null;
      }

      return (fieldDescriptor.isObjectField()) ? null : fieldDescriptor.toValueField();
   }

   private IndexFieldDescriptor getFieldDescriptor(Class<?> entityType, String[] propertyPath) {
      IndexDescriptor indexDescriptor = getIndexDescriptor(entityType);
      if (indexDescriptor == null) {
         return null;
      }

      Optional<IndexFieldDescriptor> field = indexDescriptor.field(StringHelper.join(propertyPath));
      return field.orElse(null);
   }

   private IndexDescriptor getIndexDescriptor(Class<?> type) {
      SearchIndexedEntity indexedEntity = searchMapping.indexedEntity(type);
      if (indexedEntity == null) {
         return null;
      }

      return indexedEntity.indexManager().descriptor();
   }

   public static final class SearchFieldIndexingMetadata implements IndexedFieldProvider.FieldIndexingMetadata<Class<?>> {

      private final IndexDescriptor indexDescriptor;

      public SearchFieldIndexingMetadata(IndexDescriptor indexDescriptor) {
         this.indexDescriptor = indexDescriptor;
      }

      @Override
      public boolean hasProperty(String[] propertyPath) {
         return getField(propertyPath) != null;
      }

      @Override
      public boolean isSearchable(String[] propertyPath) {
         IndexValueFieldTypeDescriptor field = getField(propertyPath);
         return field != null && field.searchable();
      }

      @Override
      public boolean isAnalyzed(String[] propertyPath) {
         IndexValueFieldTypeDescriptor field = getField(propertyPath);
         return field != null && field.analyzerName().isPresent();
      }

      @Override
      public boolean isNormalized(String[] propertyPath) {
         IndexValueFieldTypeDescriptor field = getField(propertyPath);
         return field != null && field.normalizerName().isPresent();
      }

      @Override
      public boolean isProjectable(String[] propertyPath) {
         IndexValueFieldTypeDescriptor field = getField(propertyPath);
         return field != null && field.projectable();
      }

      @Override
      public boolean isAggregable(String[] propertyPath) {
         IndexValueFieldTypeDescriptor field = getField(propertyPath);
         return field != null && field.aggregable();
      }

      @Override
      public boolean isSortable(String[] propertyPath) {
         IndexValueFieldTypeDescriptor field = getField(propertyPath);
         return field != null && field.sortable();
      }

      @Override
      public boolean isVector(String[] propertyPath) {
         IndexValueFieldTypeDescriptor field = getField(propertyPath);
         return field != null && field.traits().contains(IndexFieldTraits.Predicates.KNN);
      }

      @Override
      public Object getNullMarker(String[] propertyPath) {
         return null;
      }

      @Override
      public Class<?> keyType(String property) {
         return null;
      }

      @Override
      public boolean isSpatial(String[] propertyPath) {
         IndexValueFieldTypeDescriptor field = getField(propertyPath);
         if (field == null) {
            return false;
         }

         Set<String> traits = field.traits();
         return  (traits.contains(IndexFieldTraits.Predicates.SPATIAL_WITHIN_BOUNDING_BOX) ||
               traits.contains(IndexFieldTraits.Predicates.SPATIAL_WITHIN_CIRCLE) ||
               traits.contains(IndexFieldTraits.Predicates.SPATIAL_WITHIN_POLYGON));
      }

      private IndexValueFieldTypeDescriptor getField(String[] propertyPath) {
         Optional<IndexFieldDescriptor> field = indexDescriptor.field(StringHelper.join(propertyPath));
         if (!field.isPresent()) {
            return null;
         }

         IndexFieldDescriptor indexFieldDescriptor = field.get();
         if (!indexFieldDescriptor.isValueField()) {
            return null;
         }

         return indexFieldDescriptor.toValueField().type();
      }
   }
}
