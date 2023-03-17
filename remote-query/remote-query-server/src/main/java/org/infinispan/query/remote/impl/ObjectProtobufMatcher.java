package org.infinispan.query.remote.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.objectfilter.impl.MetadataAdapter;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;

public class ObjectProtobufMatcher extends ProtobufMatcher {

   private final AdvancedCache<?, ?> cache;

   public ObjectProtobufMatcher(SerializationContext serializationContext, IndexedFieldProvider<Descriptor> indexedFieldProvider,
                                AdvancedCache<?, ?> cache) {
      super(serializationContext, indexedFieldProvider);
      this.cache = cache;
   }

   @Override
   protected MetadataAdapter<Descriptor, FieldDescriptor, Integer> createMetadataAdapter(Descriptor messageDescriptor) {
      return new ProtobufMetadataProjectableAdapter(super.createMetadataAdapter(messageDescriptor),
            cache.withMediaType(MediaType.APPLICATION_PROTOSTREAM, MediaType.APPLICATION_PROTOSTREAM));
   }
}
