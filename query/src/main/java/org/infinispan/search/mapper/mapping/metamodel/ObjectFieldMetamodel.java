package org.infinispan.search.mapper.mapping.metamodel;

import java.util.LinkedHashMap;
import java.util.Map;

import org.hibernate.search.engine.backend.metamodel.IndexFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexObjectFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexObjectFieldTypeDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldDescriptor;
import org.hibernate.search.engine.backend.metamodel.IndexValueFieldTypeDescriptor;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public class ObjectFieldMetamodel implements JsonSerialization {

   private final boolean multiValued;
   private final boolean multiValuedInRoot;
   private final boolean nested;

   private final Map<String, ValueFieldMetamodel> valueFields = new LinkedHashMap<>();
   private final Map<String, ObjectFieldMetamodel> objectFields = new LinkedHashMap<>();

   public ObjectFieldMetamodel(boolean multiValued, boolean multiValuedInRoot, IndexObjectFieldDescriptor descriptor) {
      this.multiValued = multiValued;
      this.multiValuedInRoot = multiValuedInRoot;

      IndexObjectFieldTypeDescriptor type = descriptor.type();
      nested = type.nested();

      for (IndexFieldDescriptor field : descriptor.staticChildren()) {
         String name = field.relativeName();
         boolean fieldMultiValued = field.multiValued();
         boolean fieldMultiValuedInRoot = field.multiValuedInRoot();
         if (field.isValueField()) {
            IndexValueFieldDescriptor valueField = field.toValueField();
            IndexValueFieldTypeDescriptor fieldType = valueField.type();

            valueFields.put(name, new ValueFieldMetamodel(fieldMultiValued, fieldMultiValuedInRoot, fieldType));
         } else if (field.isObjectField()) {
            // can recur safely since the index structure is a finite depth tree (no cycles)
            objectFields.put(name, new ObjectFieldMetamodel(fieldMultiValued, fieldMultiValuedInRoot, field.toObjectField()));
         }
      }
   }

   public boolean isMultiValued() {
      return multiValued;
   }

   public boolean isMultiValuedInRoot() {
      return multiValuedInRoot;
   }

   public boolean isNested() {
      return nested;
   }

   public Map<String, ValueFieldMetamodel> getValueFields() {
      return valueFields;
   }

   public Map<String, ObjectFieldMetamodel> getObjectFields() {
      return objectFields;
   }

   @Override
   public Json toJson() {
      Json object = Json.object("multi-valued", multiValued, "multi-valued-in-root", multiValuedInRoot, "nested", nested);
      if (!valueFields.isEmpty()) {
         object.set("value-fields", Json.make(valueFields));
      }
      if (!objectFields.isEmpty()) {
         object.set("object-fields", Json.make(objectFields));
      }
      return object;
   }
}
