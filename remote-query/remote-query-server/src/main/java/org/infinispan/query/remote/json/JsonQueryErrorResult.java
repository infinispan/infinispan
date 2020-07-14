package org.infinispan.query.remote.json;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.query.remote.json.JSONConstants.CAUSE;
import static org.infinispan.query.remote.json.JSONConstants.MESSAGE;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.dataconversion.internal.Json;

public class JsonQueryErrorResult implements JsonSerialization {
   private final String message;
   private final String cause;

   public JsonQueryErrorResult(String message, String cause) {
      this.message = message;
      this.cause = cause;
   }

   @Override
   public Json toJson() {
      return Json.object().set(JSONConstants.ERROR, Json.object().set(MESSAGE, message).set(CAUSE, cause));
   }

   public byte[] asBytes() {
      return toJson().toString().getBytes(UTF_8);
   }
}
