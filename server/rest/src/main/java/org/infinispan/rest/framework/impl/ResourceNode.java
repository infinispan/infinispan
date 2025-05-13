package org.infinispan.rest.framework.impl;

import static org.infinispan.rest.framework.impl.LookupResultImpl.INVALID_ACTION;
import static org.infinispan.rest.framework.impl.LookupResultImpl.INVALID_METHOD;
import static org.infinispan.rest.framework.impl.LookupResultImpl.NOT_FOUND;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.LookupResult;
import org.infinispan.rest.framework.LookupResult.Status;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Tree structure where leaf is associated with one or more {@link Invocation}.
 *
 * @since 10.0
 */
class ResourceNode {

   private static final Log logger = LogFactory.getLog(ResourceNode.class, Log.class);
   public static final StringPathItem WILDCARD_PATH = new StringPathItem("*");

   private final PathItem pathItem;
   private final Map<ExtendedMethod, Invocation> invocationTable = new HashMap<>();
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
               Invocation previous = invocationTable.put(new ExtendedMethod(method), invocation);
               if (previous != null) {
                  throw logger.duplicateResourceMethod(invocation.getName(), m, pathItem.toString());
               }
            });
         } else {
            invocation.methods().forEach(m -> invocationTable.put(new ExtendedMethod(m.toString(), action), invocation));
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

   public String dumpTree() {
      StringBuilder stringBuilder = new StringBuilder();
      dumpTree(stringBuilder, this, 0);
      return stringBuilder.toString();
   }

   @Override
   public String toString() {
      return "ResourceNode{" +
            "pathItem=" + pathItem +
            ", invocationTable=" + invocationTable +
            ", children=" + children +
            '}';
   }

   private void dumpTree(StringBuilder builder, ResourceNode node, int ident) {
      for (int i = 0; i < ident; i++) builder.append("    ");
      if (!node.pathItem.getPath().equals("/")) builder.append("/").append(node.pathItem);
      else builder.append(node.pathItem);
      node.invocationTable.forEach((k, v) -> builder.append(" ").append(k).append(":").append(v));
      builder.append("\n");
      node.children.forEach((key, value) -> dumpTree(builder, value, ident + 1));
   }

   private void insertPathInternal(ResourceNode node, Invocation invocation, List<PathItem> path) {
      if (path.size() == 1) {
         PathItem next = path.iterator().next();
         if (next.getPath().equals("/")) {
            node.updateTable(invocation);
            return;
         }
         ResourceNode child = node.children.get(next);

         ResourceNode conflict = getConflicts(node, next);
         if (conflict != null) {
            throw logger.duplicateResource(next.toString(), invocation, conflict.toString());
         }
         if (child == null) {
            node.insert(next, invocation);
         } else {
            child.updateTable(invocation);
         }
      } else {
         PathItem pathItem = path.iterator().next();
         if (pathItem.getPath().equals("/")) {
            insertPathInternal(node, invocation, path.subList(1, path.size()));
         } else {
            ResourceNode child = node.children.get(pathItem);
            if (child == null) {
               ResourceNode inserted = node.insert(pathItem, null);
               insertPathInternal(inserted, invocation, path.subList(1, path.size()));
            } else {
               if (!child.pathItem.getPath().equals(pathItem.getPath())) {
                  throw logger.duplicateResource(pathItem.toString(), invocation, child.toString());
               }

               insertPathInternal(child, invocation, path.subList(1, path.size()));
            }
         }
      }
   }

   private ResourceNode findMatch(String path, Map<String, String> variables) {
      for (Map.Entry<PathItem, ResourceNode> e : children.entrySet()) {
         if (e.getKey() instanceof VariablePathItem vpi) {
            Map<String, String> vars = PathInterpreter.resolveVariables(vpi.getExpression(), path);
            if (!vars.isEmpty()) {
               variables.putAll(vars);
               return e.getValue();
            }
         }
      }
      return null;
   }

   private ResourceNode getConflicts(ResourceNode node, PathItem candidate) {
      Map<PathItem, ResourceNode> children = node.children;
      for (Map.Entry<PathItem, ResourceNode> entry : children.entrySet()) {
         PathItem pathItem = entry.getKey();
         ResourceNode resourceNode = entry.getValue();
         if (pathItem.getClass() != candidate.getClass()) return resourceNode;
      }
      return null;
   }

   LookupResult find(Method method, List<PathItem> path, String action) {
      ResourceNode current = this;
      Map<String, String> variables = new HashMap<>();
      boolean root = true;
      for (PathItem pathItem : path) {
         if (pathItem.equals(current.pathItem) && root) {
            root = false;
            continue;
         }
         ResourceNode resourceNode = current.children.get(pathItem);
         ResourceNode matchAll = current.children.get(WILDCARD_PATH);
         if (resourceNode != null) {
            current = resourceNode;
         } else {
            if (matchAll != null) {
               current = matchAll;
               break;
            }
            ResourceNode variableMatch = current.findMatch(pathItem.getPath(), variables);
            if (variableMatch == null) return NOT_FOUND;
            current = variableMatch;
         }
      }
      if (current.invocationTable.isEmpty()) return NOT_FOUND;

      ExtendedMethod dispatchMethod = new ExtendedMethod(method.toString(), action);
      Invocation invocation = current.invocationTable.get(dispatchMethod);

      if (invocation != null) return new LookupResultImpl(invocation, variables, Status.FOUND);

      if (current.invocationTable.keySet().stream().anyMatch(p -> p.method.equals(method.toString()))) {
         return INVALID_ACTION;
      }
      return INVALID_METHOD;
   }

   private static class ExtendedMethod {
      final String method;
      final String action;

      ExtendedMethod(String method, String action) {
         this.method = method;
         this.action = action;
      }

      ExtendedMethod(String method) {
         this.method = method;
         this.action = null;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ExtendedMethod that = (ExtendedMethod) o;
         return method.equals(that.method) &&
               Objects.equals(action, that.action);
      }

      @Override
      public int hashCode() {
         return Objects.hash(method, action);
      }
   }
}
