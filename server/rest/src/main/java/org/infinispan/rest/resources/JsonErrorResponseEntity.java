package org.infinispan.rest.resources;

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
class JsonErrorResponseEntity {

   @JsonProperty(MESSAGE)
   private String message;

   @JsonProperty(CAUSE)
   private String cause;

   public JsonErrorResponseEntity(String message, String cause) {
      this.message = message;
      this.cause = cause;
   }

   public JsonErrorResponseEntity() {
   }

   public String getMessage() {
      return message;
   }

   public String getCause() {
      return cause;
   }
}
