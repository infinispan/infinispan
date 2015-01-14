package org.infinispan.query.remote.indexing;

import java.util.Set;

/**
 * All fields of Protobuf types are indexed and stored by default. This behaviour is usually acceptable in most cases
 * but it can become a performance problem if there are many or very large fields. To avoid such problems Infinispan allows you
 * to specify which fields to index and store by means of two annotations ({@literal @}Indexed and {@literal @}IndexedField)
 * that can be added directly to your Protobuf schema in the documentation comments of message type definitions and
 * field definitions as demonstrated in the example below:
 * <p/>
 * <b>Example:<b/>
 * <p/>
 * <pre>
 * /*
 *  This type is indexed, but not all of its fields are.
 *  {@literal @}Indexed
 *  *{@literal /}
 * message Note {
 *
 *    /*
 *     This field is indexed but not stored. It can be used for querying but not for projections.
 *     {@literal @}IndexedField(index=true, store=false)
 *     *{@literal /}
 *     optional string text = 1;
 *
 *    /*
 *     A field that is both indexed and stored.
 *     {@literal @}IndexedField
 *     *{@literal /}
 *     optional string author = 2;
 *
 *     /* @IndexedField(index=false, store=true) *{@literal /}
 *     optional bool isRead = 3;
 *
 *     /* This field is not annotated, so it is neither indexed nor stored. *{@literal /}
 *     optional int32 priority = 4;
 * }
 * </pre>
 *
 *     Documentation annotations can be added on the last line of the documentation comment that precedes the element
 *     to be annotated (message type definition or field definition).
 *
 *     The '{@literal @}Indexed' annotation applies to message types only, has a boolean value and it defaults to 'true', so '{@literal @}Indexed' is equivalent to '{@literal @}Indexed(true)'.
 *     The presence of this annotation indicates the intention to selectively specify which of the fields of this message type are to be indexed.
 *     '@Indexed(false)' indicates that no fields will be indexed anyway, so the eventual '@IndexedField' annotations present at field level will be ignored.
 *
 *     The '{@literal @}IndexedField' annotation applies to fields only and has two boolean attributes, 'index' and 'store', which default to
 *     true ({@literal @}IndexedField is equivalent to {@literal @}IndexedField(index=true, store=true)).
 *     The 'index' attribute indicates whether the field will be indexed, so it can be used for indexed queries, while the 'store' attribute indicates
 *     whether the field value is to be stored in the index too, so it becomes useable for projections.
 * <p/>
 * <b>NOTE:</b> The {@literal @}IndexedField annotation has effect only if the containing message type was annotated as '{@literal @}Indexed'.
 *
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
