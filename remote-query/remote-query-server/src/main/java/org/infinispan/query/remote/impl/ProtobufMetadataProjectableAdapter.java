package org.infinispan.query.remote.impl;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.metadata.Metadata;
import org.infinispan.query.objectfilter.impl.MetadataAdapter;
import org.infinispan.query.objectfilter.impl.syntax.parser.ProtobufPropertyHelper;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.dsl.embedded.impl.MetadataProjectableAdapter;

public class ProtobufMetadataProjectableAdapter extends MetadataProjectableAdapter<Descriptor, FieldDescriptor, Integer> {

   public ProtobufMetadataProjectableAdapter(MetadataAdapter<Descriptor, FieldDescriptor, Integer> baseAdapter) {
      super(baseAdapter);
   }

   @Override
   public boolean isValueProjection(Integer attribute) {
      return attribute.equals(ProtobufPropertyHelper.VALUE_FIELD_ATTRIBUTE_ID);
   }

   @Override
   public Object valueProjection(Object rawValue) {
      return new WrappedMessage(rawValue);
   }

   @Override
   public Object metadataProjection(Metadata metadata, Integer attribute) {
      if (attribute.equals(ProtobufPropertyHelper.VERSION_FIELD_ATTRIBUTE_ID)) {
         EntryVersion version = metadata.version();
         if (version instanceof NumericVersion) {
            return ((NumericVersion) version).getVersion();
         }
         if (version instanceof SimpleClusteredVersion) {
            return ((SimpleClusteredVersion) version).getVersion();
         }
      }
      return null;
   }
}
