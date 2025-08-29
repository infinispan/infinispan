package org.infinispan.rest.framework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents each hop of a path. Example: /rest/a/b is formed by 3 path items: rest, a, b.
 *
 * @since 10.0
 */
public abstract class PathItem {

   public abstract String getPath();

   public static boolean hasPathParameter(String path) {
      if (path == null) return false;
      int i = path.indexOf('{');
      int j = path.indexOf('}');
      return checkPathVariableIndexes(i, j);
   }

   public static List<String> retrieveAllPathVariables(String path) {
      Objects.requireNonNull(path, "Path variable should not be null");

      boolean search = hasPathParameter(path);
      List<String> paths = null;
      while (search) {
         int i = path.indexOf('{');
         int j = path.indexOf('}');
         String variable = path.substring(i + 1, j);

         if (paths == null) paths = new ArrayList<>();

         paths.add(variable);

         if (path.length() < j + 1) break;
         path = path.substring(j + 1);
         search = hasPathParameter(path);
      }

      return paths == null
            ? Collections.emptyList()
            : paths;
   }

   private static boolean checkPathVariableIndexes(int i, int j) {
      return (i > -1 && j > -1 && j > i + 1);
   }
}
