package org.infinispan.search.mapper.mapping.metamodel;

import java.util.LinkedHashMap;
import java.util.Map;

import org.hibernate.search.engine.backend.metamodel.IndexDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldTypeDescriptor;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.search.mapper.mapping.SearchIndexedEntity;

public class IndexMetamodel implements JsonSerialization {

   private final String entityName;
   private final String javaClassName;
   private final String indexName;
   private final Map<String, ValueFieldMetamodel> valueFields = new LinkedHashMap<>();
   private final Map<String, ObjectFieldMetamodel> objectFields = new LinkedHashMap<>();

   public IndexMetamodel(SearchIndexedEntity indexedEntity) {
      IndexDescriptor descriptor = indexedEntity.indexManager().descriptor();

      entityName = indexedEntity.name();
      javaClassName = indexedEntity.javaClass().getName();
      indexName = descriptor.hibernateSearchName();

      for (IndexFieldDescriptor field : descriptor.staticFields()) {
         String name = field.relativeName();
         boolean multiValued = field.multiValued();
         boolean multiValuedInRoot = field.multiValuedInRoot();
         if (field.isValueField()) {
            IndexValueFieldDescriptor valueField = field.toValueField();
            IndexValueFieldTypeDescriptor type = valueField.type();
            valueFields.put(name, new ValueFieldMetamodel(multiValued, multiValuedInRoot, type));
         } else if (field.isObjectField()) {
            objectFields.put(name, new ObjectFieldMetamodel(multiValued, multiValuedInRoot, field.toObjectField()));
         }
      }
   }

   public String getEntityName() {
      return entityName;
   }

   public String getJavaClassName() {
      return javaClassName;
   }

   public String getIndexName() {
      return indexName;
   }

   public Map<String, ValueFieldMetamodel> getValueFields() {
      return valueFields;
   }

   public Map<String, ObjectFieldMetamodel> getObjectFields() {
      return objectFields;
   }

   @Override
   public Json toJson() {
      Json object = Json.object("entity-name", Json.make(entityName),
            "java-class", Json.make(javaClassName),
            "index-name", Json.make(indexName));

      if (!valueFields.isEmpty()) {
         object.set("value-fields", Json.make(valueFields));
      }
      if (!objectFields.isEmpty()) {
         object.set("object-fields", Json.make(objectFields));
      }
      return object;
   }
}
