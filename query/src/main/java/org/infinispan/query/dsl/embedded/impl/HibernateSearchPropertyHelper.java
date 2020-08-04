package org.infinispan.query.dsl.embedded.impl;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;

import org.apache.lucene.document.DateTools;
import org.hibernate.search.engine.backend.metamodel.IndexDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldTypeDescriptor;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionPropertyHelper;
import org.infinispan.objectfilter.impl.util.StringHelper;
import org.infinispan.search.mapper.mapping.SearchIndexedEntity;
import org.infinispan.search.mapper.mapping.SearchMappingHolder;

public class HibernateSearchPropertyHelper extends ReflectionPropertyHelper {

   public static String KEY = "__ISPN_Key";
   public static String VALUE = "__HSearch_This";

   private final SearchMappingHolder searchMapping;

   public HibernateSearchPropertyHelper(SearchMappingHolder searchMapping, EntityNameResolver<Class<?>> entityNameResolver) {
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
      if (!(Date.class.equals(type))) {
         return super.convertToPropertyType(entityType, propertyPath, value);
      }

      try {
         return DateTools.stringToDate(value);
      } catch (ParseException e) {
         throw new ParsingException(e);
      }
   }

   @Override
   public Class<?> getPrimitivePropertyType(Class<?> entityType, String[] propertyPath) {
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
   public boolean isRepeatedProperty(Class<?> entityType, String[] propertyPath) {
      IndexFieldDescriptor fieldDescriptor = getFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return super.isRepeatedProperty(entityType, propertyPath);
      }

      if (fieldDescriptor.isValueField() && fieldDescriptor.toValueField().multiValued()) {
         return true;
      }
      if (!fieldDescriptor.isValueField() && fieldDescriptor.toObjectField().multiValued()) {
         return true;
      }

      if (propertyPath.length == 1) {
         return false;
      }

      return isRepeatedProperty(entityType, Arrays.copyOfRange(propertyPath, 0, propertyPath.length - 1));
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

      if (propertyPath.length == 1 && propertyPath[0].equals(KEY)) {
         return true;
      }
      if (propertyPath.length == 1 && propertyPath[0].equals(VALUE)) {
         return true;
      }

      return super.hasProperty(entityType, propertyPath);
   }

   @Override
   public IndexedFieldProvider<Class<?>> getIndexedFieldProvider() {
      return entityType -> {
         IndexDescriptor indexDescriptor = getIndexDescriptor(entityType);
         if (indexDescriptor == null) {
            return IndexedFieldProvider.NO_INDEXING;
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
      return (field.isPresent()) ? field.get() : null;
   }

   private IndexDescriptor getIndexDescriptor(Class<?> type) {
      SearchIndexedEntity indexedEntity = searchMapping.getSearchMapping().indexedEntity(type);
      if (indexedEntity == null) {
         return null;
      }

      return indexedEntity.indexManager().descriptor();
   }

   private static class SearchFieldIndexingMetadata implements IndexedFieldProvider.FieldIndexingMetadata {

      private final IndexDescriptor indexDescriptor;

      public SearchFieldIndexingMetadata(IndexDescriptor indexDescriptor) {
         this.indexDescriptor = indexDescriptor;
      }

      @Override
      public boolean isIndexed(String[] propertyPath) {
         IndexValueFieldTypeDescriptor field = getField(propertyPath);
         return (field == null) ? false : field.isSearchable();
      }

      @Override
      public boolean isAnalyzed(String[] propertyPath) {
         IndexValueFieldTypeDescriptor field = getField(propertyPath);
         return (field == null) ? false : field.analyzerName().isPresent();
      }

      @Override
      public boolean isStored(String[] propertyPath) {
         IndexValueFieldTypeDescriptor field = getField(propertyPath);
         return (field == null) ? false : field.isProjectable();
      }

      @Override
      public Object getNullMarker(String[] propertyPath) {
         return null;
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
