package org.infinispan.query.remote.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.metadata.Metadata;
import org.infinispan.objectfilter.impl.MetadataAdapter;
import org.infinispan.objectfilter.impl.syntax.parser.ProtobufPropertyHelper;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.query.dsl.embedded.impl.MetadataProjectableAdapter;

public class ProtobufMetadataProjectableAdapter extends MetadataProjectableAdapter<Descriptor, FieldDescriptor, Integer> {

   public ProtobufMetadataProjectableAdapter(MetadataAdapter<Descriptor, FieldDescriptor, Integer> baseAdapter, AdvancedCache<?, ?> cache) {
      super(baseAdapter, cache);
   }

   @Override
   public Object projection(CacheEntry<?, ?> cacheEntry, Integer attribute) {
      if (ProtobufPropertyHelper.VALUE_FIELD_ATTRIBUTE_ID == attribute) {
         return new WrappedMessage(cacheEntry.getValue());
      }

      Metadata metadata = cacheEntry.getMetadata();
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
