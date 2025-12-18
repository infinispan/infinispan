package org.infinispan.rest.framework.openapi;

import java.util.Collection;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.rest.framework.ResourceDescription;

/**
 * This is the root document object of the OpenAPI document.
 *
 * @param openapi: A <b>required</b> field with the SemVer of the OpenAPI specification.
 * @param info:    A <b>required</b> field that contains metadata about the API.
 * @param paths:   A <b>required</b> property with the paths and operation available in the API.
 * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.1.1.md">OpenAPI Specification</a>
 */
public record OpenAPIDocument(
      String openapi,
      Info info,
      Paths paths,
      Components components,
      Collection<ResourceDescription> resources
) implements JsonSerialization {

   @Override
   public Json toJson() {
      Json tags = Json.array();
      for (ResourceDescription resource : resources) {
         Json tag = Json.object()
               .set("name", resource.group())
               .set("description", resource.description());
         tags.add(tag);
      }
      return Json.object()
            .set("openapi", openapi)
            .set("info", info)
            .set("paths", paths)
            .set("components", components)
            .set("tags", tags);
   }
}
