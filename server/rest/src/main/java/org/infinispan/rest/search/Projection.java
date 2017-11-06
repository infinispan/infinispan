package org.infinispan.rest.search;

import static org.infinispan.rest.JSONConstants.HIT;

import java.util.Map;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @since 9.2
 */
public class Projection {

   @JsonProperty(HIT)
   private Map<String, Object> value;

   Projection(Map<String, Object> value) {
      this.value = value;
   }

   public Map<String, Object> getValue() {
      return value;
   }
}
