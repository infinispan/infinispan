package org.infinispan.rest.framework.openapi;

import java.util.Objects;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * Describe the parameters of a single operation.
 * <p>
 * The parameter is uniquely identified by the name and location.
 * </p>
 *
 * @param name     A <b>required</b> case-sensitive property.
 * @param in
 * @param required
 * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.1.1.md#parameter-object">Parameter object schema</a>
 */
public record Parameter(
      String name,
      ParameterIn in,
      boolean required,
      Schema schema,
      String description
) implements JsonSerialization {

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Parameter parameter = (Parameter) o;
      return Objects.equals(name, parameter.name) && in == parameter.in;
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, in);
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("name", name)
            .set("in", in.toString())
            .set("required", required)
            .set("schema", schema)
            .set("description", description);
   }
}
