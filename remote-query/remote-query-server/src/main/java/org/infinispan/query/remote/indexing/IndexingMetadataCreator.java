package org.infinispan.query.remote.indexing;

import org.infinispan.protostream.AnnotationMetadataCreator;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;

import java.util.HashSet;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class IndexingMetadataCreator implements AnnotationMetadataCreator<IndexingMetadata, Descriptor> {

   // Recognized annotations:
   // @Indexed (single boolean argument, default true)
   // @IndexedField (index = true/false, default true, store = true/false, default true)
   @Override
   public IndexingMetadata create(Descriptor descriptor, AnnotationElement.Annotation annotation) {
      AnnotationElement.Value indexedValue = annotation.getDefaultAttributeValue();
      if (Boolean.TRUE.equals(indexedValue.getValue())) {
         Set<Integer> indexedFields = new HashSet<Integer>();
         Set<Integer> storedFields = new HashSet<Integer>();
         for (FieldDescriptor fd : descriptor.getFields()) {
            AnnotationElement.Annotation indexedFieldAnnotation = fd.getAnnotations().get(IndexingMetadata.INDEXED_FIELD_ANNOTATION);
            if (indexedFieldAnnotation != null) {
               AnnotationElement.Value indexAttribute = indexedFieldAnnotation.getAttributeValue(IndexingMetadata.INDEX_ATTRIBUTE);
               if (Boolean.TRUE.equals(indexAttribute.getValue())) {
                  indexedFields.add(fd.getNumber());
               }
               AnnotationElement.Value storeAttribute = indexedFieldAnnotation.getAttributeValue(IndexingMetadata.STORE_ATTRIBUTE);
               if (Boolean.TRUE.equals(storeAttribute.getValue())) {
                  storedFields.add(fd.getNumber());
               }
            }
         }
         return new IndexingMetadata(true, indexedFields, storedFields);
      } else {
         return new IndexingMetadata(false, null, null);
      }
   }
}