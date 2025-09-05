package org.infinispan.rest.framework.openapi;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * The {@link Info} object holds metadata about the API.
 *
 * @param title:       A <b>required</b> property with the title of the API.
 * @param description: A <i>optional</i> property with a description about the API.
 * @param license:     A <i>optional</i> property with the license information about the software.
 * @param version:     A <b>required</b> property with the software version.
 * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.1.1.md#info-object">Info object schema</a>
 */
public record Info(
      String title,
      String description,
      License license,
      String version
) implements JsonSerialization {
   @Override
   public Json toJson() {
      return Json.object()
            .set("title", title)
            .set("description", description)
            .set("version", version);
   }
}
