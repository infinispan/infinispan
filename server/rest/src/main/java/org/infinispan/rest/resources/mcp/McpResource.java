package org.infinispan.rest.resources.mcp;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record McpResource(String uri, String name, String description, String mimeType) implements JsonSerialization {

   @Override
   public Json toJson() {
      Json json = Json.object()
            .set("uri", uri)
            .set(McpConstants.NAME, name)
            .set(McpConstants.DESCRIPTION, description);
      if (mimeType != null) {
         json.set("mimeType", mimeType);
      }
      return json;
   }
}
