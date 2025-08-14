package org.infinispan.rest.framework.openapi;

import java.util.Objects;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.rest.framework.Method;

/**
 * Holds information about the relative paths and operations.
 * <p>
 * This class holds information about each path and parameters. Holding all the information necessary to recreate the
 * tree of objects defined in the schema without additional objects.
 * </p>
 *
 * @param path   A <b>required</b> property. The relative path (starting with `/`) to an individual endpoint.
 * @param method A <b>required</b> property holding the HTTP method information.
 * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.1.1.md#paths-object">Paths object schema.</a>
 */
public record Path(
      String path,
      Method method,
      Operation operation
) implements JsonSerialization, Comparable<Path> {

   @Override
   public Json toJson() {
      return Json.object(method.name().toLowerCase(), operation);
   }

   @Override
   public int compareTo(Path o) {
      int compare = path.compareTo(o.path);
      return compare == 0 ? method.compareTo(o.method) : compare;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      Path path1 = (Path) o;
      return Objects.equals(path, path1.path) && method == path1.method;
   }

   @Override
   public int hashCode() {
      return Objects.hash(path, method);
   }
}
