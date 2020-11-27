package org.infinispan.query.remote.impl.mapping.typebridge;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.hibernate.search.engine.backend.document.IndexObjectFieldReference;
import org.hibernate.search.engine.backend.document.model.dsl.IndexSchemaElement;
import org.hibernate.search.engine.backend.document.model.dsl.IndexSchemaObjectField;
import org.hibernate.search.mapper.pojo.bridge.binding.TypeBindingContext;
import org.hibernate.search.mapper.pojo.bridge.mapping.programmatic.TypeBinder;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.remote.impl.mapping.reference.GlobalReferenceHolder;
import org.infinispan.query.remote.impl.mapping.reference.IndexReferenceHolder;
import org.infinispan.query.remote.impl.mapping.reference.MessageReferenceProvider;

public class ProtobufMessageBinder implements TypeBinder {

   // TODO make this variable configurable with `@IndexedEmbedded` annotation
   public static final int MAX_DEPTH = 7;

   private final GlobalReferenceHolder globalReferenceHolder;
   private final String rootMessageName;

   public ProtobufMessageBinder(GlobalReferenceHolder globalReferenceHolder, String rootMessageName) {
      this.globalReferenceHolder = globalReferenceHolder;
      this.rootMessageName = rootMessageName;
   }

   @Override
   public void bind(TypeBindingContext context) {
      context.dependencies().useRootOnly();
      IndexReferenceHolder indexReferenceProvider = createIndexReferenceProvider(context, MAX_DEPTH);
      Descriptor descriptor = globalReferenceHolder.getDescriptor(rootMessageName);
      context.bridge(byte[].class, new ProtobufMessageBridge(indexReferenceProvider, descriptor));
   }

   private IndexReferenceHolder createIndexReferenceProvider(TypeBindingContext context, int maxDepth) {
      final Map<String, IndexFieldReference<?>> fieldReferenceMap = new HashMap<>();
      final Map<String, IndexObjectFieldReference> objectReferenceMap = new HashMap<>();

      Stack<State> stack = new Stack<>();
      stack.push(new State(globalReferenceHolder.getMessageReferenceProviders().get(rootMessageName),
            "", context.indexSchemaElement(), 0));

      while (!stack.isEmpty()) {
         State currentState = stack.pop();
         fieldReferenceMap.putAll(currentState.bind());

         if (currentState.depth == maxDepth) {
            continue;
         }

         for (MessageReferenceProvider.Embedded embedded : currentState.messageReferenceProvider.getEmbedded()) {
            String newPath = ("".equals(currentState.path)) ? embedded.getFieldName() :
                  currentState.path + "." + embedded.getFieldName();

            String typeName = embedded.getTypeFullName();
            MessageReferenceProvider messageReferenceProvider = globalReferenceHolder.getMessageReferenceProviders().get(typeName);

            IndexSchemaObjectField indexSchemaElement = currentState.indexSchemaElement.objectField(embedded.getFieldName());
            if (embedded.isRepeated()) {
               indexSchemaElement.multiValued();
            }

            objectReferenceMap.put(newPath, indexSchemaElement.toReference());
            State state = new State(messageReferenceProvider, newPath, indexSchemaElement, currentState.depth + 1);

            stack.push(state);
         }
      }

      return new IndexReferenceHolder(fieldReferenceMap, objectReferenceMap);
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
   }
}
