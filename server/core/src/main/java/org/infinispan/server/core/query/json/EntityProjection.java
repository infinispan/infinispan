package org.infinispan.server.core.query.json;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.infinispan.commons.dataconversion.internal.Json;

public class EntityProjection extends Hit {

   public EntityProjection(Object value) {
      super(value);
   }

   @Override
   public Json toJson() {
      String rawJson = value instanceof String ? value.toString() : new String((byte[]) value, UTF_8);
      return Json.factory().raw(rawJson);
   }
}
