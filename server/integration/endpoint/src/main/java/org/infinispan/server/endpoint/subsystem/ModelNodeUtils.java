package org.infinispan.server.endpoint.subsystem;

import org.jboss.dmr.ModelNode;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author gustavonalle
 * @since 8.1
 */
public class ModelNodeUtils {

   static ModelNode addToList(ModelNode listNode, ModelNode elements) {
      if (elements == null || !elements.isDefined())
         return listNode;
      if (!listNode.isDefined()) {
         listNode.setEmptyList();
      }
      ModelNode result = listNode.clone();

      elements.asList().stream().map(ModelNode::asString).forEach(toAdd -> {
         boolean existent = result.asList().stream().anyMatch(m -> m.asString().equals(toAdd));
         if (!existent) {
            result.add().set(toAdd);
         }
      });

      return result;
   }

   static boolean contains(ModelNode list, String value) {
      return list.isDefined() && list.asList().stream().anyMatch(modelNode -> modelNode.asString().contains(value));
   }

   static ModelNode removeFromList(ModelNode original, ModelNode elements) {
      if (elements == null || !elements.isDefined() || !original.isDefined())
         return original;
      ModelNode result = new ModelNode();
      Set<String> toRemove = elements.asList().stream().map(ModelNode::asString).collect(Collectors.toSet());
      original.asList().stream().map(ModelNode::asString).filter(e -> !toRemove.contains(e)).forEach(result::add);
      return result;
   }
}
