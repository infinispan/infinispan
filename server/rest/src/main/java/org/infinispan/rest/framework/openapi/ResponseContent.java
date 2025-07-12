package org.infinispan.rest.framework.openapi;

import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

import io.netty.handler.codec.http.HttpResponseStatus;

public record ResponseContent(
      String description,
      HttpResponseStatus status,
      Map<MediaType, Schema> responses
) implements JsonSerialization {

   @Override
   public Json toJson() {
      Json content = Json.object();
      for (Map.Entry<MediaType, Schema> entry : responses.entrySet()) {
         content.set(entry.getKey().toString(), Json.object("schema", entry.getValue()));
      }
      return Json.object()
            .set("description", description)
            .set("content", content);
   }
}
