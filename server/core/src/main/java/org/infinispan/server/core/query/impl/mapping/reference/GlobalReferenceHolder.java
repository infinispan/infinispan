package org.infinispan.server.core.query.impl.mapping.reference;

import static org.infinispan.server.core.query.impl.indexing.IndexingMetadata.findProcessedAnnotation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.GenericDescriptor;
import org.infinispan.server.core.query.impl.indexing.IndexingMetadata;
import org.infinispan.server.core.query.impl.indexing.search5.Search5MetadataCreator;

public final class GlobalReferenceHolder {

   private final Map<String, MessageReferenceProvider> messageReferenceProviders = new HashMap<>();
   private final Set<RootMessageInfo> rootMessages = new LinkedHashSet<>();
   private final Map<String, Descriptor> rootDescriptors = new HashMap<>();
   private final Map<String, Descriptor> notRootDescriptors = new HashMap<>();

   public GlobalReferenceHolder(Map<String, GenericDescriptor> descriptors) {
      Set<Descriptor> messageTypes = new HashSet<>();
      Set<Descriptor> nestedDescriptors = new HashSet<>();

      for (Map.Entry<String, GenericDescriptor> entry : descriptors.entrySet()) {
         GenericDescriptor genericDescriptor = entry.getValue();
         if (!(genericDescriptor instanceof Descriptor descriptor)) {
            // skip enum types, they are mapped as strings
            continue;
         }

         MessageReferenceProvider messageReferenceProvider = new MessageReferenceProvider(descriptor);
         if (messageReferenceProvider.isEmpty()) {
            notRootDescriptors.put(descriptor.getFullName(), descriptor);
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

   public MessageReferenceProvider messageReferenceProvider(String fullName) {
      return messageReferenceProviders.get(fullName);
   }

   public boolean hasKeyMapping(String fullName) {
      MessageReferenceProvider messageReferenceProvider = messageReferenceProvider(fullName);
      if (messageReferenceProvider == null) {
         return false;
      }
      return messageReferenceProvider.keyMessageName() != null;
   }

   @Override
   public String toString() {
      return messageReferenceProviders.toString();
   }

   public MessageReferenceProvider messageProviderForEmbeddedType(MessageReferenceProvider.Embedded embedded) {
      String typeFullName = embedded.getTypeFullName();
      messageReferenceProviders.computeIfAbsent(typeFullName, (key) -> {
         Descriptor descriptor = notRootDescriptors.get(typeFullName);
         IndexingMetadata indexingMetadata = Search5MetadataCreator.createForEmbeddedType(descriptor);
         return new MessageReferenceProvider(descriptor, indexingMetadata);
      });
      MessageReferenceProvider provider = messageReferenceProviders.get(typeFullName);
      embedded.indexingMetadata(provider.indexingMetadata());
      return provider;
   }

   public static final class RootMessageInfo {
      private final String fullName;
      private final String indexName;

      private RootMessageInfo(Descriptor descriptor) {
         IndexingMetadata indexingMetadata =  findProcessedAnnotation(descriptor, IndexingMetadata.INDEXED_ANNOTATION);
         this.fullName = descriptor.getFullName();
         this.indexName = indexingMetadata.indexName() != null ? indexingMetadata.indexName() : fullName;
      }

      public String getFullName() {
         return fullName;
      }

      public String getIndexName() {
         return indexName;
      }
   }
}
