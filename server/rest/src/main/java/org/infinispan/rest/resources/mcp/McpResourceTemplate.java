package org.infinispan.rest.resources.mcp;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record McpResourceTemplate(String uriTemplate, String name, String description, String mimeType) implements JsonSerialization {

   @Override
   public Json toJson() {
      Json json = Json.object()
            .set("uriTemplate", uriTemplate)
            .set(McpConstants.NAME, name)
            .set(McpConstants.DESCRIPTION, description);
      if (mimeType != null) {
         json.set("mimeType", mimeType);
      }
      return json;
   }
}
