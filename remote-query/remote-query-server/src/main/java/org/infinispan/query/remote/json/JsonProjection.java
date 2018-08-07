package org.infinispan.query.remote.json;

import static org.infinispan.query.remote.json.JSONConstants.HIT;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @since 9.4
 */
public class JsonProjection {

   @JsonProperty(HIT)
   private Map<String, Object> value;

   JsonProjection(Map<String, Object> value) {
      this.value = value;
   }

   public Map<String, Object> getValue() {
      return value;
   }
}
