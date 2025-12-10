package org.infinispan.spring.common.session;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.spring.common.session.MapSessionProtoAdapter.SessionAttribute;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Protostream adapter for {@link SessionUpdateRemappingFunction}.
 *
 * <p>Attribute values set by the application should be marshalled with Protostream, but Java Serialization
 * is also supported.</p>
 * <p>Attribute values set by spring-session internally have not been converted to use Protostream,
 * so they are always marshalled using Java Serialization.</p>
 * <p>Note: Each attribute value uses either Protostream or Java Serialization for marshalling.
 * Mixing Protostream and Java Serialization in the same attribute is not supported.</p>
 */
@ProtoAdapter(SessionUpdateRemappingFunction.class)
@ProtoTypeId(ProtoStreamTypeIds.SPRING_SESSION_REMAP)
public class SessionUpdateRemappingFunctionProtoAdapter {
   @ProtoFactory
   static SessionUpdateRemappingFunction createFunction(Collection<SessionAttribute> attributes, Instant lastAccessedTime, Long maxInactiveSeconds) {
      SessionUpdateRemappingFunction function = new SessionUpdateRemappingFunction();
      function.setLastAccessedTime(lastAccessedTime);
      if (maxInactiveSeconds != null) {
         function.setMaxInactiveInterval(Duration.ofSeconds(maxInactiveSeconds));
      }
      Map<String,Object> delta = new HashMap<>();
      for (SessionAttribute attribute : attributes) {
         delta.put(attribute.getName(), attribute.getValue());
      }
      function.setDelta(delta);
      return function;
   }

   @ProtoField(number = 1)
   Instant getLastAccessedTime(SessionUpdateRemappingFunction function) {
      return function.getLastAccessedTime();
   }

   @ProtoField(number = 2)
   Long getMaxInactiveSeconds(SessionUpdateRemappingFunction function) {
      return function.getMaxInactiveInterval() == null ? null : function.getMaxInactiveInterval().getSeconds();
   }

   @ProtoField(number = 3)
   Collection<SessionAttribute> getAttributes(SessionUpdateRemappingFunction function) {
      if (function.getDelta() == null) {
         return Collections.emptyList();
      }
      return function.getDelta().entrySet().stream()
            .map(entry -> new SessionAttribute(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
   }
}
