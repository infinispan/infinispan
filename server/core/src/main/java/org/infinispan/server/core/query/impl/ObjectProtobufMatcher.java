package org.infinispan.server.core.query.impl;

import org.infinispan.query.objectfilter.impl.MetadataAdapter;
import org.infinispan.query.objectfilter.impl.ProtobufMatcher;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;

public class ObjectProtobufMatcher extends ProtobufMatcher {

   public ObjectProtobufMatcher(SerializationContext serializationContext, RemoteHibernateSearchPropertyHelper propertyHelper) {
      super(serializationContext, propertyHelper);
   }

   @Override
   protected MetadataAdapter<Descriptor, FieldDescriptor, Integer> createMetadataAdapter(Descriptor messageDescriptor) {
      return new ProtobufMetadataProjectableAdapter(super.createMetadataAdapter(messageDescriptor));
   }
}
