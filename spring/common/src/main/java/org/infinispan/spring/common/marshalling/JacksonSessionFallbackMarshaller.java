package org.infinispan.spring.common.marshalling;

import java.io.IOException;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.spring.common.session.SessionFallbackResolver;
import org.springframework.security.jackson2.SecurityJackson2Modules;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Session attribute fallback marshaller that uses Jackson with Spring Security modules.
 * <p>
 * This marshaller supports Spring Security types commonly stored in sessions
 * (like Authentication objects) via Jackson serialization.
 * <p>
 * <strong>Usage Note:</strong> This utility class is available for configurable session fallback
 * serialization but is not automatically wired into the Spring Boot auto-configuration layer
 * in the current implementation. The default session fallback remains {@link SpringJavaSerializationMarshaller}.
 * This class can be used via {@link SessionFallbackResolver#resolve(String, ClassLoader)} or by custom
 * configurations.
 *
 * @since 16.3
 */
public class JacksonSessionFallbackMarshaller extends AbstractMarshaller {

   private final ObjectMapper objectMapper;

   public JacksonSessionFallbackMarshaller(ClassLoader classLoader) {
      this.objectMapper = new ObjectMapper();
      this.objectMapper.registerModules(SecurityJackson2Modules.getModules(classLoader));
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException {
      byte[] bytes = objectMapper.writeValueAsBytes(o);
      return ByteBufferImpl.create(bytes, 0, bytes.length);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length)
         throws IOException, ClassNotFoundException {
      return objectMapper.readValue(buf, offset, length, Object.class);
   }

   @Override
   public boolean isMarshallable(Object o) {
      return o != null;
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_JSON;
   }
}
