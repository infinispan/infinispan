package org.infinispan.query.remote.impl.indexing.infinispan;

import org.infinispan.protostream.AnnotationMetadataCreator;
import org.infinispan.protostream.descriptors.AnnotationElement;
import org.infinispan.protostream.descriptors.FieldDescriptor;

public class IndexingMetadataHolderFactory implements AnnotationMetadataCreator<IndexingMetadataHolder, FieldDescriptor> {

   @Override
   public IndexingMetadataHolder create(FieldDescriptor annotatedDescriptor, AnnotationElement.Annotation annotation) {
      return new IndexingMetadataHolder();
   }
}
