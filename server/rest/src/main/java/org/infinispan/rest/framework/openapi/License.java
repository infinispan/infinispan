package org.infinispan.rest.framework.openapi;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * License information for the exposed API.
 *
 * @param name: A <b>required</b> property with the license's name.
 * @param url:  An <i>optional</i> property with a URL pointing to the license.
 * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.1.1.md#license-object">License object schema.</a>
 */
public record License(
      String name,
      String url
) implements JsonSerialization {

   @Override
   public Json toJson() {
      return Json.object()
            .set("name", name)
            .set("url", url);
   }
}
