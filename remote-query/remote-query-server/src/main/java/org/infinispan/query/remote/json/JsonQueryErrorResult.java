package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.CAUSE;
import static org.infinispan.query.remote.json.JSONConstants.ERROR;
import static org.infinispan.query.remote.json.JSONConstants.MESSAGE;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonPropertyOrder({MESSAGE, CAUSE})
@JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_OBJECT, use = JsonTypeInfo.Id.NAME)
@JsonTypeName(ERROR)
@SuppressWarnings("unused")
public class JsonQueryErrorResult extends JsonQueryResponse {

   @JsonProperty(MESSAGE)
   private String message;

   @JsonProperty(CAUSE)
   private String cause;

   public JsonQueryErrorResult(String message, String cause) {
      this.message = message;
      this.cause = cause;
   }

   public JsonQueryErrorResult() {
   }

   public String getMessage() {
      return message;
   }

   public String getCause() {
      return cause;
   }
}
