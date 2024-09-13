package org.infinispan.query.remote.impl;

import org.infinispan.objectfilter.impl.MetadataAdapter;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;

public class ObjectProtobufMatcher extends ProtobufMatcher {

   public ObjectProtobufMatcher(SerializationContext serializationContext, IndexedFieldProvider<Descriptor> indexedFieldProvider) {
      super(serializationContext, indexedFieldProvider);
   }

   @Override
   protected MetadataAdapter<Descriptor, FieldDescriptor, Integer> createMetadataAdapter(Descriptor messageDescriptor) {
      return new ProtobufMetadataProjectableAdapter(super.createMetadataAdapter(messageDescriptor));
   }
}
