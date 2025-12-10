package org.infinispan.spring.common.session;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.BaseMarshallerDelegate;
import org.infinispan.protostream.GeneratedMarshallerBase;
import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.impl.SerializationContextImpl;
import org.springframework.session.MapSession;

/**
 * Protostream adapter for Spring's {@link MapSession}.
 *
 * <p>Attribute values set by the application should be marshalled with Protostream, but Java Serialization
 * is also supported.</p>
 * <p>Attribute values set by spring-session internally have not been converted to use Protostream,
 * so they are always marshalled using Java Serialization.</p>
 * <p>Note: Each attribute value uses either Protostream or Java Serialization for marshalling.
 * Mixing Protostream and Java Serialization in the same attribute is not supported.</p>
 *
 * @author Dan Berindei
 * @since 12.1
 */
@ProtoAdapter(MapSession.class)
@ProtoTypeId(ProtoStreamTypeIds.SPRING_SESSION)
public class MapSessionProtoAdapter {
   @ProtoFactory
   static MapSession createSession(String id, String originalId, Collection<SessionAttribute> attributes, Instant creationTime, Instant lastAccessedTime, long maxInactiveSeconds) {
      MapSession session = new MapSession(originalId);
      session.setId(id);
      session.setCreationTime(creationTime);
      session.setLastAccessedTime(lastAccessedTime);
      session.setMaxInactiveInterval(Duration.ofSeconds(maxInactiveSeconds));

      for (SessionAttribute attribute : attributes) {
         session.setAttribute(attribute.getName(), attribute.getValue());
      }
      return session;
   }

   @ProtoField(number = 1)
   String getId(MapSession session) {
      return session.getId();
   }

   @ProtoField(number = 2)
   String getOriginalId(MapSession session) {
      if (Objects.equals(session.getOriginalId(), session.getId()))
         return null;

      return session.getOriginalId();
   }

   @ProtoField(number = 3, defaultValue = "0")
   Instant getCreationTime(MapSession session) {
      return session.getCreationTime();
   }

   @ProtoField(number = 4, defaultValue = "0")
   Instant getLastAccessedTime(MapSession session) {
      return session.getLastAccessedTime();
   }

   @ProtoField(number = 5, defaultValue = "-1")
   long getMaxInactiveSeconds(MapSession session) {
      return session.getMaxInactiveInterval().getSeconds();
   }

   @ProtoField(number = 6)
   Collection<SessionAttribute> getAttributes(MapSession session) {
      return session.getAttributeNames().stream()
                    .map(name -> new SessionAttribute(name, session.getAttribute(name)))
                    .collect(Collectors.toList());
   }

   @ProtoTypeId(ProtoStreamTypeIds.SPRING_SESSION_ATTRIBUTE)
   public static class SessionAttribute {
      private final String name;
      private final Object value;

      @ProtoFactory
      SessionAttribute(String name, WrappedMessage wrappedMessage, byte[] serializedBytes) {
         throw new IllegalStateException("Custom marshaller not registered!");
      }

      public SessionAttribute(String name, Object value) {
         this.name = name;
         this.value = value;
      }

      @ProtoField(number = 1)
      public String getName() {
         return name;
      }

      public Object getValue() {
         return value;
      }

      @ProtoField(number = 2)
      public WrappedMessage getWrappedMessage() {
         return new WrappedMessage(value);
      }

      @ProtoField(number = 3)
      public byte[] getSerializedBytes() {
         throw new IllegalStateException("Custom marshaller not registered!");
      }
   }

   /**
    * Generated with protostream-processor and then adapted to use {@code JavaSerializationMarshaller}.
    *
    * <p>A raw marshaller is necessary because we need a {@code JavaSerializationMarshaller} instance,
    * and {@link MapSessionProtoAdapter} must be stateless.</p>
    */
   public static final class SessionAttributeRawMarshaller extends GeneratedMarshallerBase
         implements ProtobufTagMarshaller<SessionAttribute> {

      private final JavaSerializationMarshaller javaSerializationMarshaller;
      private BaseMarshallerDelegate<WrappedMessage> wrappedMessageDelegate;

      public SessionAttributeRawMarshaller(JavaSerializationMarshaller javaSerializationMarshaller) {
         this.javaSerializationMarshaller = javaSerializationMarshaller;
      }

      @Override
      public Class<MapSessionProtoAdapter.SessionAttribute> getJavaClass() {
         return MapSessionProtoAdapter.SessionAttribute.class;
      }

      @Override
      public String getTypeName() {
         return "org.infinispan.persistence.spring.session.SessionAttribute";
      }

      @Override
      public MapSessionProtoAdapter.SessionAttribute read(ReadContext ctx) throws IOException {
         TagReader in = ctx.getReader();
         String name = null;
         boolean done = false;
         Object value = null;
         while (!done) {
            final int tag = in.readTag();
            switch (tag) {
               case 0:
                  done = true;
                  break;
               case 10: {
                  name = in.readString();
                  break;
               }
               case 18: {
                  if (wrappedMessageDelegate == null) {
                     wrappedMessageDelegate = ctx.getSerializationContext().getMarshallerDelegate(WrappedMessage.class);
                  }
                  int length = in.readUInt32();
                  int oldLimit = in.pushLimit(length);
                  WrappedMessage wrappedMessage = readMessage(wrappedMessageDelegate, ctx);
                  value = wrappedMessage.getValue();
                  in.checkLastTagWas(0);
                  in.popLimit(oldLimit);
                  break;
               }
               case 26: {
                  byte[] serializedBytes = in.readByteArray();
                  value = deserializeValue(serializedBytes);
                  break;
               }
               default: {
                  if (!in.skipField(tag)) done = true;
               }
            }
         }
         return new MapSessionProtoAdapter.SessionAttribute(name, value);
      }

      @Override
      public void write(WriteContext ctx, MapSessionProtoAdapter.SessionAttribute attribute) throws IOException {
         final String name = attribute.getName();
         if (name != null) ctx.getWriter().writeString(1, name);

         Object value = attribute.getValue();
         if (value != null) {
            if (ctx.getSerializationContext().canMarshall(value.getClass())) {
               // The attribute value is an application class that can be marshalled with Protostream
               final WrappedMessage wrappedMessage = new WrappedMessage(value);
               if (wrappedMessageDelegate == null) {
                  wrappedMessageDelegate = ((SerializationContextImpl) ctx.getSerializationContext()).getMarshallerDelegate(WrappedMessage.class);
               }
               writeNestedMessage(wrappedMessageDelegate, ctx, 2, wrappedMessage);
            } else {
               // The attribute value cannot be marshalled with Protostream, but Java Serialization should work
               // E.g. all sessions have a org.springframework.security.core.context.SecurityContext attribute
               byte[] serializedBytes = serializeValue(value);
               ctx.getWriter().writeBytes(3, serializedBytes);
            }
         }
      }

      private byte[] serializeValue(Object value) throws IOException {
         final byte[] serializedBytes;
         try {
            serializedBytes = javaSerializationMarshaller.objectToByteBuffer(value);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheException(e);
         }
         return serializedBytes;
      }

      private Object deserializeValue(byte[] serializedBytes) {
         try {
            return javaSerializationMarshaller.objectFromByteBuffer(serializedBytes);
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
      }
   }
}
