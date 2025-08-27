package org.infinispan.server.core.query.impl.mapping.typebridge;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.hibernate.search.engine.backend.document.IndexObjectFieldReference;
import org.hibernate.search.engine.backend.document.model.dsl.IndexSchemaElement;
import org.hibernate.search.engine.backend.document.model.dsl.IndexSchemaObjectField;
import org.hibernate.search.engine.backend.types.ObjectStructure;
import org.hibernate.search.mapper.pojo.bridge.binding.TypeBindingContext;
import org.hibernate.search.mapper.pojo.bridge.mapping.programmatic.TypeBinder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.server.core.query.impl.logging.Log;
import org.infinispan.server.core.query.impl.mapping.reference.GlobalReferenceHolder;
import org.infinispan.server.core.query.impl.mapping.reference.IndexReferenceHolder;
import org.infinispan.server.core.query.impl.mapping.reference.MessageReferenceProvider;
import org.infinispan.server.core.query.impl.mapping.type.ProtobufKeyValuePair;

public class ProtobufMessageBinder implements TypeBinder {

   private static final Log log = LogFactory.getLog(ProtobufMessageBinder.class, Log.class);

   private final GlobalReferenceHolder globalReferenceHolder;
   private final String rootMessageName;

   public ProtobufMessageBinder(GlobalReferenceHolder globalReferenceHolder, String rootMessageName) {
      this.globalReferenceHolder = globalReferenceHolder;
      this.rootMessageName = rootMessageName;
   }

   @Override
   public void bind(TypeBindingContext context) {
      context.dependencies().useRootOnly();
      MessageReferenceProvider messageReferenceProvider = globalReferenceHolder.messageReferenceProvider(rootMessageName);
      String keyMessageName = messageReferenceProvider.keyMessageName();
      String keyPropertyName = messageReferenceProvider.keyPropertyName();

      IndexReferenceHolder indexReferenceProvider = createIndexReferenceProvider(context);
      Descriptor valueDescriptor = globalReferenceHolder.getDescriptor(rootMessageName);
      if (keyMessageName == null) {
         context.bridge(byte[].class, new ProtobufMessageBridge(indexReferenceProvider, valueDescriptor));
         return;
      }

      Descriptor keyDescriptor = globalReferenceHolder.getDescriptor(keyMessageName);
      context.bridge(ProtobufKeyValuePair.class, new ProtobufKeyValueBridge(indexReferenceProvider, keyPropertyName,
            keyDescriptor, valueDescriptor));
   }

   private IndexReferenceHolder createIndexReferenceProvider(TypeBindingContext context) {
      final Map<String, IndexFieldReference<?>> fieldReferenceMap = new HashMap<>();
      final Map<String, IndexObjectFieldReference> objectReferenceMap = new HashMap<>();
      final Map<String, IndexReferenceHolder.GeoIndexFieldReference> geoReferenceMap = new HashMap<>();

      Stack<State> stack = new Stack<>();
      stack.push(new State(globalReferenceHolder.getMessageReferenceProviders().get(rootMessageName),
            "", context.indexSchemaElement(), 0));
      Integer maxDepth = null;

      while (!stack.isEmpty()) {
         State currentState = stack.pop();
         fieldReferenceMap.putAll(currentState.bind());
         geoReferenceMap.putAll(currentState.bindGeo());

         if (maxDepth != null && currentState.depth == maxDepth) {
            continue;
         }

         for (MessageReferenceProvider.Embedded embedded : currentState.messageReferenceProvider.getEmbedded()) {
            String newPath = ("".equals(currentState.path)) ? embedded.getFieldName() :
                  currentState.path + "." + embedded.getFieldName();

            maxDepth = embedded.getIncludeDepth();
            MessageReferenceProvider messageReferenceProvider = globalReferenceHolder
                  .messageProviderForEmbeddedType(embedded);

            ObjectStructure structure = embedded.getStructure();
            if (structure == null) {
               structure = ObjectStructure.FLATTENED; // for legacy annotations
            }
            IndexSchemaObjectField indexSchemaElement = currentState.indexSchemaElement
                  .objectField(embedded.getFieldName(), structure);
            if (embedded.isRepeated()) {
               indexSchemaElement.multiValued();
            }

            objectReferenceMap.put(newPath, indexSchemaElement.toReference());
            State state = new State(messageReferenceProvider, newPath, indexSchemaElement, currentState.depth + 1);

            stack.push(state);
         }
      }

      return new IndexReferenceHolder(fieldReferenceMap, objectReferenceMap, geoReferenceMap);
   }

   private static class State {
      private final MessageReferenceProvider messageReferenceProvider;
      private final String path;
      private final IndexSchemaElement indexSchemaElement;
      private final int depth;

      public State(MessageReferenceProvider messageReferenceProvider, String path,
                   IndexSchemaElement indexSchemaElement, int depth) {
         this.messageReferenceProvider = messageReferenceProvider;
         this.path = path;
         this.indexSchemaElement = indexSchemaElement;
         this.depth = depth;
      }

      public Map<String, IndexFieldReference<?>> bind() {
         return messageReferenceProvider.bind(indexSchemaElement, path);
      }

      public Map<String, IndexReferenceHolder.GeoIndexFieldReference> bindGeo() {
         return messageReferenceProvider.bindGeo(indexSchemaElement, path);
      }
   }
}
