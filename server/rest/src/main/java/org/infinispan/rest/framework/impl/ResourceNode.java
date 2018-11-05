package org.infinispan.rest.framework.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Tree structure where leaf is associated with one or more {@link Invocation}.
 *
 * @since 10.0
 */
class ResourceNode {

   private final static Log logger = LogFactory.getLog(ResourceNode.class, Log.class);

   private final PathItem pathItem;
   private final Map<String, Invocation> invocationTable = new HashMap<>();
   private final Map<PathItem, ResourceNode> children = new HashMap<>();

   ResourceNode(PathItem pathItem, Invocation invocation) {
      this.pathItem = pathItem;
      this.updateTable(invocation);
   }

   private void updateTable(Invocation invocation) {
      if (invocation != null) {
         String action = invocation.getAction();
         if (action == null) {
            invocation.methods().forEach(m -> {
               String method = m.toString();
               Invocation previous = invocationTable.put(method, invocation);
               if (previous != null) {
                  throw logger.duplicateResource(invocation.getName(), m, pathItem.toString());
               }
            });
         } else {
            invocation.methods().forEach(m -> invocationTable.put(m.toString() + "_" + action, invocation));
         }
      }
   }

   private ResourceNode insert(PathItem label, Invocation invocation) {
      if (!children.containsKey(label)) {
         ResourceNode value = new ResourceNode(label, invocation);
         children.put(label, value);
         return value;
      }
      return null;
   }

   void insertPath(Invocation invocation, List<PathItem> path) {
      insertPathInternal(this, invocation, path);
   }

   @Override
   public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      dumpTree(stringBuilder, this, 0);
      return stringBuilder.toString();
   }


   private void dumpTree(StringBuilder builder, ResourceNode node, int ident) {
      for (int i = 0; i < ident; i++) builder.append("    ");
      builder.append("/").append(node.pathItem);
      node.invocationTable.forEach((k, v) -> builder.append(" ").append(k).append(":").append(v));
      builder.append("\n");
      node.children.forEach((key, value) -> dumpTree(builder, value, ident + 1));
   }

   private void insertPathInternal(ResourceNode node, Invocation invocation, List<PathItem> path) {
      if (path.size() == 1) {
         PathItem next = path.iterator().next();
         if(next.getPath().isEmpty()) {
            updateTable(invocation);
            return;
         }
         ResourceNode child = node.children.get(next);
         if (child == null) {
            node.insert(next, invocation);
         } else {
            child.updateTable(invocation);
         }
      } else {
         PathItem pathItem = path.iterator().next();
         ResourceNode child = node.children.get(pathItem);
         if (child == null) {
            ResourceNode inserted = node.insert(pathItem, null);
            insertPathInternal(inserted, invocation, path.subList(1, path.size()));
         } else {
            insertPathInternal(child, invocation, path.subList(1, path.size()));
         }
      }
   }

   private ResourceNode findMatch(String path, Map<String, String> variables) {
      for (Map.Entry<PathItem, ResourceNode> e : children.entrySet()) {
         if (e.getKey() instanceof VariablePathItem) {
            VariablePathItem vpi = (VariablePathItem) e.getKey();
            Map<String, String> vars = PathInterpreter.resolveVariables(vpi.getExpression(), path);
            if (!vars.isEmpty()) {
               variables.putAll(vars);
               return e.getValue();
            }
         }
      }
      return null;
   }

   public LookupResult find(Method method, List<PathItem> path, String action) {
      ResourceNode current = this;
      Map<String, String> variables = new HashMap<>();
      for (PathItem pathItem : path) {
         ResourceNode resourceNode = current.children.get(pathItem);
         if (resourceNode != null) {
            current = resourceNode;
         } else {
            ResourceNode variableMatch = current.findMatch(pathItem.getPath(), variables);
            if (variableMatch == null) return null;
            current = variableMatch;
         }
      }
      String dispatchMethod = action == null ? method.toString() : method.toString() + "_" + action;
      Invocation invocation = current.invocationTable.get(dispatchMethod);
      if (invocation == null) return null;
      return new LookupResultImpl(invocation, variables);
   }
}
