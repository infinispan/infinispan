package org.infinispan.query.remote.impl.mapping.reference;

import static org.infinispan.query.remote.impl.indexing.IndexingMetadata.findProcessedAnnotation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.GenericDescriptor;
import org.infinispan.query.remote.impl.indexing.IndexingMetadata;

public class GlobalReferenceHolder {

   private final Map<String, MessageReferenceProvider> messageReferenceProviders = new HashMap<>();
   private final Set<RootMessageInfo> rootMessages = new LinkedHashSet<>();
   private final Map<String, Descriptor> rootDescriptors = new HashMap<>();

   public GlobalReferenceHolder(Map<String, GenericDescriptor> descriptors) {
      HashSet<Descriptor> messageTypes = new HashSet<>();
      HashSet<Descriptor> nestedDescriptors = new HashSet<>();

      for (Map.Entry<String, GenericDescriptor> entry : descriptors.entrySet()) {
         GenericDescriptor genericDescriptor = entry.getValue();
         if (!(genericDescriptor instanceof Descriptor)) {
            // skip enum types, they are mapped as strings
            continue;
         }

         Descriptor descriptor = (Descriptor) genericDescriptor;
         MessageReferenceProvider messageReferenceProvider = new MessageReferenceProvider(descriptor);
         if (messageReferenceProvider.isEmpty()) {
            // skip not indexed types
            continue;
         }

         messageReferenceProviders.put(entry.getKey(), messageReferenceProvider);
         messageTypes.add(descriptor);
         nestedDescriptors.addAll(descriptor.getNestedTypes());
      }

      messageTypes.removeAll(nestedDescriptors);
      for (Descriptor descriptor : messageTypes) {
         rootMessages.add(new RootMessageInfo(descriptor));
         rootDescriptors.put(descriptor.getFullName(), descriptor);
      }
   }

   public Map<String, MessageReferenceProvider> getMessageReferenceProviders() {
      return messageReferenceProviders;
   }

   public Set<RootMessageInfo> getRootMessages() {
      return rootMessages;
   }

   public Descriptor getDescriptor(String fullName) {
      return rootDescriptors.get(fullName);
   }

   @Override
   public String toString() {
      return messageReferenceProviders.toString();
   }

   public static class RootMessageInfo {
      private final String fullName;
      private final String indexName;

      private RootMessageInfo(Descriptor descriptor) {
         IndexingMetadata indexingMetadata =  findProcessedAnnotation(descriptor, IndexingMetadata.INDEXED_ANNOTATION);
         this.fullName = descriptor.getFullName();
         this.indexName = (indexingMetadata.indexName() != null) ? indexingMetadata.indexName() : fullName;
      }

      public String getFullName() {
         return fullName;
      }

      public String getIndexName() {
         return indexName;
      }
   }
}
