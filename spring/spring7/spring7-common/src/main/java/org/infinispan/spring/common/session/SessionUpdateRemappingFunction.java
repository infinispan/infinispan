package org.infinispan.spring.common.session;

import org.springframework.session.MapSession;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.BiFunction;

public class SessionUpdateRemappingFunction implements BiFunction<String, MapSession, MapSession>, Serializable {

   private static final long serialVersionUID = 1L;

   private Instant lastAccessedTime;
   private Duration maxInactiveInterval;
   private Map<String, Object> delta;

   @Override
   public MapSession apply(final String key, final MapSession value) {
      if (value == null) {
         return null;
      }
      if (this.lastAccessedTime != null) {
         value.setLastAccessedTime(this.lastAccessedTime);
      }
      if (this.maxInactiveInterval != null) {
         value.setMaxInactiveInterval(this.maxInactiveInterval);
      }
      if (this.delta != null) {
         for (final Map.Entry<String, Object> attribute : this.delta.entrySet()) {
            if (attribute.getValue() != null) {
               value.setAttribute(attribute.getKey(), attribute.getValue());
            } else {
               value.removeAttribute(attribute.getKey());
            }
         }
      }
      return value;
   }

   Instant getLastAccessedTime() {
      return lastAccessedTime;
   }

   void setLastAccessedTime(final Instant lastAccessedTime) {
      this.lastAccessedTime = lastAccessedTime;
   }

   Duration getMaxInactiveInterval() {
      return maxInactiveInterval;
   }

   void setMaxInactiveInterval(final Duration maxInactiveInterval) {
      this.maxInactiveInterval = maxInactiveInterval;
   }

   Map<String, Object> getDelta() {
      return delta;
   }

   void setDelta(final Map<String, Object> delta) {
      this.delta = delta;
   }
}
