package org.infinispan.query.remote.impl;

import static org.infinispan.query.remote.impl.indexing.IndexingMetadata.findProcessedAnnotation;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.lucene.document.DateTools;
import org.hibernate.search.engine.backend.metamodel.IndexDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldTypeDescriptor;
import org.hibernate.search.engine.backend.types.IndexFieldTraits;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.query.objectfilter.ParsingException;
import org.infinispan.query.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.query.objectfilter.impl.syntax.parser.ProtobufPropertyHelper;
import org.infinispan.query.objectfilter.impl.util.StringHelper;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.GenericDescriptor;
import org.infinispan.protostream.descriptors.JavaType;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;
import org.infinispan.query.mapper.mapping.SearchIndexedEntity;
import org.infinispan.query.mapper.mapping.SearchMapping;

public final class RemoteHibernateSearchPropertyHelper extends ProtobufPropertyHelper {

   public static RemoteHibernateSearchPropertyHelper create(SerializationContext serializationContext,
                                                            SearchMapping searchMapping) {
      RemoteIndexFieldProvider indexedFieldProvider = new RemoteIndexFieldProvider(serializationContext, searchMapping);
      return new RemoteHibernateSearchPropertyHelper(serializationContext, searchMapping, indexedFieldProvider);
   }

   private static final String KEY = "__ISPN_Key";

   private final SearchMapping searchMapping;

   public RemoteHibernateSearchPropertyHelper(SerializationContext serializationContext, SearchMapping searchMapping,
                                              IndexedFieldProvider<Descriptor> indexedFieldProvider) {
      super(serializationContext, indexedFieldProvider);
      this.searchMapping = searchMapping;
   }

   @Override
   public Object convertToPropertyType(Descriptor entityType, String[] propertyPath, String value) {
      IndexValueFieldDescriptor fieldDescriptor = getValueFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return super.convertToPropertyType(entityType, propertyPath, value);
      }

      Class<?> type = fieldDescriptor.type().dslArgumentClass();
      if (Date.class == type) {
         try {
            return DateTools.stringToDate(value);
         } catch (ParseException e) {
            throw new ParsingException(e);
         }
      }

      return super.convertToPropertyType(entityType, propertyPath, value);
   }

   @Override
   public Class<?> getPrimitivePropertyType(Descriptor entityType, String[] propertyPath) {
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
   public Class<?> getIndexedPropertyType(Descriptor entityType, String[] propertyPath) {
      IndexValueFieldDescriptor fieldDescriptor = getValueFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return null;
      }

      return fieldDescriptor.type().dslArgumentClass();
   }

   @Override
   public boolean isNestedIndexStructure(Descriptor entityType, String[] propertyPath) {
      IndexFieldDescriptor fieldDescriptor = getFieldDescriptor(entityType, propertyPath);
      return fieldDescriptor != null && fieldDescriptor.type().traits().contains(IndexFieldTraits.Predicates.NESTED);
   }

   @Override
   public boolean isRepeatedProperty(Descriptor entityType, String[] propertyPath) {
      IndexFieldDescriptor fieldDescriptor = getFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return super.isRepeatedProperty(entityType, propertyPath);
      }
      return fieldDescriptor.multiValuedInRoot();
   }

   @Override
   public boolean hasEmbeddedProperty(Descriptor entityType, String[] propertyPath) {
      IndexFieldDescriptor fieldDescriptor = getFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return super.hasEmbeddedProperty(entityType, propertyPath);
      }

      return fieldDescriptor.isObjectField();
   }

   @Override
   public boolean hasProperty(Descriptor entityType, String[] propertyPath) {
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

   private IndexValueFieldDescriptor getValueFieldDescriptor(Descriptor entityType, String[] propertyPath) {
      IndexFieldDescriptor fieldDescriptor = getFieldDescriptor(entityType, propertyPath);
      if (fieldDescriptor == null) {
         return null;
      }

      return (fieldDescriptor.isObjectField()) ? null : fieldDescriptor.toValueField();
   }

   private IndexFieldDescriptor getFieldDescriptor(Descriptor entityType, String[] propertyPath) {
      IndexDescriptor indexDescriptor = getIndexDescriptor(entityType);
      if (indexDescriptor == null) {
         return null;
      }

      Optional<IndexFieldDescriptor> field = indexDescriptor.field(StringHelper.join(propertyPath));
      return field.orElse(null);
   }

   private IndexDescriptor getIndexDescriptor(Descriptor type) {
      if (searchMapping == null) {
         return null;
      }

      SearchIndexedEntity indexedEntity = searchMapping.indexedEntity(type.getFullName());
      if (indexedEntity == null) {
         return null;
      }

      return indexedEntity.indexManager().descriptor();
   }

   public static class RemoteIndexFieldProvider implements IndexedFieldProvider<Descriptor> {

      private final SerializationContext serializationContext;
      private final SearchMapping searchMapping;

      public RemoteIndexFieldProvider(SerializationContext serializationContext, SearchMapping searchMapping) {
         this.serializationContext = serializationContext;
         this.searchMapping = searchMapping;
      }

      @Override
      public FieldIndexingMetadata<Descriptor> get(Descriptor messageDescriptor) {
         if (searchMapping == null) {
            return new ProtobufFieldIndexingMetadata(messageDescriptor, serializationContext.getGenericDescriptors());
         }

         SearchIndexedEntity indexedEntity = searchMapping.indexedEntity(messageDescriptor.getFullName());
         if (indexedEntity == null) {
            return new ProtobufFieldIndexingMetadata(messageDescriptor, serializationContext.getGenericDescriptors());
         }

         IndexDescriptor descriptor = indexedEntity.indexManager().descriptor();
         return new RemoteHibernateSearchPropertyHelper.SearchFieldIndexingMetadata(
               descriptor, messageDescriptor, serializationContext.getGenericDescriptors());
      }
   }

   public static final class SearchFieldIndexingMetadata implements IndexedFieldProvider.FieldIndexingMetadata<Descriptor> {

      private final Descriptor messageDescriptor;
      private final IndexDescriptor indexDescriptor;
      private final String keyProperty;
      private final Descriptor keyMessageDescriptor;

      public SearchFieldIndexingMetadata(IndexDescriptor indexDescriptor, Descriptor messageDescriptor, Map<String, GenericDescriptor> genericDescriptors) {
         if (messageDescriptor == null) {
            throw new IllegalArgumentException("argument cannot be null");
         }
         this.indexDescriptor = indexDescriptor;
         this.messageDescriptor = messageDescriptor;
         IndexingMetadata indexingMetadata = findProcessedAnnotation(messageDescriptor, IndexingMetadata.INDEXED_ANNOTATION);
         if (indexingMetadata != null && indexingMetadata.indexingKey() != null) {
            keyProperty = indexingMetadata.indexingKey().fieldName();
            keyMessageDescriptor = (Descriptor) genericDescriptors.get(indexingMetadata.indexingKey().typeFullName());
         } else {
            keyProperty = null;
            keyMessageDescriptor = null;
         }
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
         Descriptor md = messageDescriptor;
         int i = 0;
         for (String p : propertyPath) {
            i++;
            FieldDescriptor field = md.findFieldByName(p);
            if (field == null) {
               break;
            }
            if (i == propertyPath.length) {
               IndexingMetadata indexingMetadata = findProcessedAnnotation(md, IndexingMetadata.INDEXED_ANNOTATION);
               return indexingMetadata == null ? null : indexingMetadata.getNullMarker(field.getName());
            }
            if (field.getJavaType() != JavaType.MESSAGE) {
               break;
            }
            md = field.getMessageType();
         }
         return null;
      }

      @Override
      public Descriptor keyType(String property) {
         return (property.equals(keyProperty)) ? keyMessageDescriptor : null;
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
