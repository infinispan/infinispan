package org.infinispan.query.remote.indexing;

import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class IndexingMetadata {

   public static final String INDEXED_ANNOTATION = "Indexed";
   public static final String INDEXED_FIELD_ANNOTATION = "IndexedField";
   public static final String INDEX_ATTRIBUTE = "index";
   public static final String STORE_ATTRIBUTE = "store";

   private final boolean isIndexed;
   private final Set<Integer> indexedFields;
   private final Set<Integer> storedFields;

   IndexingMetadata(boolean isIndexed, Set<Integer> indexedFields, Set<Integer> storedFields) {
      this.isIndexed = isIndexed;
      this.indexedFields = indexedFields;
      this.storedFields = storedFields;
   }

   public boolean isIndexed() {
      return isIndexed;
   }

   public boolean isFieldIndexed(int fieldId) {
      return indexedFields == null ? isIndexed : indexedFields.contains(fieldId);
   }

   public boolean isFieldStored(int fieldId) {
      return storedFields == null ? isIndexed : storedFields.contains(fieldId);
   }
}
