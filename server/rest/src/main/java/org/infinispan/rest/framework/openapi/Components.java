package org.infinispan.rest.framework.openapi;

import java.util.Map;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

public record Components(Map<String, Schema> schemas) implements JsonSerialization {
   @Override
   public Json toJson() {
      return Json.object().set("schemas", schemas);
   }
}
