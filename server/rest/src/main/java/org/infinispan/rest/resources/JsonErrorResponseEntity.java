package org.infinispan.rest.resources;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.dataconversion.internal.Json;

class JsonErrorResponseEntity implements JsonSerialization {

   private final String message;

   private final String cause;

   public JsonErrorResponseEntity(String message, String cause) {
      this.message = message;
      this.cause = cause;
   }

   @Override
   public Json toJson() {
      return Json.object("error").set("message", message).set("cause", cause);
   }
}
