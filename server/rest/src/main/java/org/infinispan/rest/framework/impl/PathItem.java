package org.infinispan.rest.framework.impl;

/**
 * Represents each hop of a path. Example: /rest/a/b is formed by 3 path items: rest, a, b.
 *
 * @since 10.0
 */
abstract class PathItem {

   static PathItem fromString(String path) {
      if (isValidExpression(path)) return new VariablePathItem(path);
      return new StringPathItem(path);
   }

   public abstract String getPath();

   private static boolean isValidExpression(String path) {
      if (path == null) return false;
      int i = path.indexOf('{');
      int j = path.indexOf('}');
      return (i > -1 && j > -1 && j > i + 1);
   }

}
