package org.infinispan.query.remote.impl.mapping.reference;

import java.util.Map;

import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.hibernate.search.engine.backend.document.IndexObjectFieldReference;

public class IndexReferenceHolder {

   private final Map<String, IndexFieldReference<?>> fieldReferenceMap;
   private final Map<String, IndexObjectFieldReference> objectReferenceMap;

   public IndexReferenceHolder(Map<String, IndexFieldReference<?>> fieldReferenceMap, Map<String, IndexObjectFieldReference> objectReferenceMap) {
      this.fieldReferenceMap = fieldReferenceMap;
      this.objectReferenceMap = objectReferenceMap;
   }

   public IndexFieldReference<?> getFieldReference(String absoluteFieldPath) {
      return fieldReferenceMap.get(absoluteFieldPath);
   }

   public IndexObjectFieldReference getObjectReference(String absoluteObjectFieldPath) {
      return objectReferenceMap.get(absoluteObjectFieldPath);
   }
}
