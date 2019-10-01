package org.infinispan.server.core.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.StandardConversions.convertTextToObject;
import static org.infinispan.commons.dataconversion.StandardConversions.decodeOctetStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Transcode between application/x-protostream and commons formats
 *
 * @since 10.0
 */
public class ProtostreamTranscoder extends OneToManyTranscoder {

   protected final static Log logger = LogFactory.getLog(JBossMarshallingTranscoder.class, Log.class);
   private final List<SerializationContext> contexts = new ArrayList<>();
   private final ProtoStreamMarshaller marshaller;
   private final ClassLoader classLoader;

   public ProtostreamTranscoder(SerializationContext ctx, ClassLoader classLoader) {
      super(APPLICATION_PROTOSTREAM, APPLICATION_OCTET_STREAM, TEXT_PLAIN, APPLICATION_OBJECT, APPLICATION_JSON, APPLICATION_UNKNOWN);
      this.contexts.add(ctx);
      this.marshaller = new ProtoStreamMarshaller(ctx);
      this.classLoader = classLoader;
   }

   public void addSerializationContext(SerializationContext ctx) {
      this.contexts.add(ctx);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(MediaType.APPLICATION_PROTOSTREAM)) {
            Object decoded = content;
            if (contentType.match(APPLICATION_OBJECT)) {
               decoded = StandardConversions.decodeObjectContent(content, contentType);
               Optional<String> wrappedParam = destinationType.getParameter("wrapped");
               if (!wrappedParam.isPresent() || !wrappedParam.get().equals("false"))
                  return marshallAsWrappedByteArray(decoded);
               return marshall(decoded);
            }
            if (contentType.match(APPLICATION_OCTET_STREAM)) {
               decoded = decodeOctetStream(content, destinationType);
            }
            if (contentType.match(TEXT_PLAIN)) {
               decoded = convertTextToObject(content, contentType);
            }
            if (contentType.match(APPLICATION_JSON)) {
               return fromJson(content, contentType);
            }
            if (contentType.match(APPLICATION_UNKNOWN) || contentType.match(APPLICATION_PROTOSTREAM)) {
               return content;
            }
            return marshallAsWrappedByteArray(decoded);
         }
         if (destinationType.match(MediaType.APPLICATION_OCTET_STREAM)) {
            Object unmarshalled = content instanceof byte[] ? unmarshallWrappedByteArray((byte[]) content) : content;
            if (unmarshalled instanceof byte[]) {
               return unmarshalled;
            }
            return StandardConversions.convertJavaToOctetStream(unmarshalled, MediaType.APPLICATION_OBJECT, marshaller);
         }
         if (destinationType.match(MediaType.TEXT_PLAIN)) {
            String decoded = unmarshallWrappedByteArray((byte[]) content).toString();
            return decoded.getBytes(destinationType.getCharset());
         }
         if (destinationType.match(MediaType.APPLICATION_OBJECT)) {
            String type = destinationType.getClassType();
            if (type == null) {
               return unmarshallWrappedByteArray((byte[]) content);
            }
            Class<?> destination = Util.loadClass(type, classLoader);
            byte[] bytes = (byte[]) content;
            return unmarshall(bytes, destination);
         }
         if (destinationType.match(MediaType.APPLICATION_JSON)) {
            String converted = toJson((byte[]) content);
            String convertType = destinationType.getClassType();
            if (convertType == null)
               return StandardConversions.convertCharset(converted, contentType.getCharset(), destinationType.getCharset());
            if (destinationType.hasStringType())
               return converted;
         }
         if (destinationType.equals(APPLICATION_UNKNOWN)) {
            //TODO: Remove wrapping of byte[] into WrappedByteArray from the Hot Rod Multimap operations.
            if (content instanceof WrappedByteArray) return content;
            return StandardConversions.convertJavaToOctetStream(content, MediaType.APPLICATION_OBJECT, marshaller);
         }
         throw logger.unsupportedContent(ProtostreamTranscoder.class.getSimpleName(), content);
      } catch (InterruptedException | IOException e) {
         throw logger.errorTranscoding(ProtostreamTranscoder.class.getSimpleName(), e);
      }
   }

   private byte[] marshallAsWrappedByteArray(Object o) throws IOException {
      for (SerializationContext ctx : contexts) {
         if (ProtoStreamMarshaller.isMarshallable(ctx, o)) {
            return ProtobufUtil.toWrappedByteArray(ctx, o);
         }
      }
      throw logger.unsupportedContent(ProtostreamTranscoder.class.getSimpleName(), o);
   }

   private byte[] marshall(Object o) throws IOException {
      for (SerializationContext ctx : contexts) {
         if (ProtoStreamMarshaller.isMarshallable(ctx, o)) {
            return ProtobufUtil.toByteArray(ctx, o);
         }
      }
      throw logger.unsupportedContent(ProtostreamTranscoder.class.getSimpleName(), o);
   }

   private Object unmarshallWrappedByteArray(byte[] bytes) throws IOException {
      Iterator<SerializationContext> it = contexts.iterator();
      while (it.hasNext()) {
         try {
            return ProtobufUtil.fromWrappedByteArray(it.next(), bytes);
         } catch (IllegalArgumentException e) {
            // IllegalArgumentException thrown if bytes can't be unmarshalled, only rethrow if all contexts have been attempted
            if (!it.hasNext())
               throw e;
         }
      }
      return null;
   }

   private Object unmarshall(byte[] bytes, Class clazz) throws IOException {
      Iterator<SerializationContext> it = contexts.iterator();
      while (it.hasNext()) {
         try {
            SerializationContext ctx = it.next();
            if (isMarshallable(ctx, clazz))
               return ProtobufUtil.fromByteArray(ctx, bytes, clazz);
         } catch (IllegalArgumentException e) {
            // IllegalArgumentException thrown if bytes can't be unmarshalled, only rethrow if all contexts have been attempted
            if (!it.hasNext())
               throw e;
         }
      }
      return null;
   }

   private String toJson(byte[] bytes) throws IOException {
      Iterator<SerializationContext> it = contexts.iterator();
      while (it.hasNext()) {
         try {
            return ProtobufUtil.toCanonicalJSON(it.next(), bytes);
         } catch (IllegalArgumentException e) {
            if (!it.hasNext())
               throw e;
         }
      }
      return null;
   }

   private byte[] fromJson(Object content, MediaType contentType) throws IOException {
      content = addTypeIfNeeded(content, contentType);
      boolean byteArray = content instanceof byte[];
      Iterator<SerializationContext> it = contexts.iterator();
      while (it.hasNext()) {
         try {
            Reader reader;
            if (byteArray) {
               reader = new InputStreamReader(new ByteArrayInputStream((byte[]) content));
            } else {
               reader = new StringReader(content.toString());
            }
            return ProtobufUtil.fromCanonicalJSON(it.next(), reader);
         } catch (IllegalArgumentException e) {
            if (!it.hasNext())
               throw e;
         }
      }
      return null;
   }

   private Object addTypeIfNeeded(Object content, MediaType type) {
      String wrapped = "{ \"_type\":\"%s\", \"_value\":\"%s\"}";
      if (content instanceof Integer || content instanceof Short) {
         return String.format(wrapped, "int32", content);
      }
      if (content instanceof Long) {
         return String.format(wrapped, "int64", content);
      }
      if (content instanceof Double) {
         return String.format(wrapped, "double", content);
      }
      if (content instanceof Float) {
         return String.format(wrapped, "float", content);
      }
      if (content instanceof Boolean) {
         return String.format(wrapped, "bool", content);
      }
      if (content instanceof String && !(content.toString()).contains("_type")) {
         return String.format(wrapped, "string", content);
      }
      return content;
   }

   private boolean isMarshallable(SerializationContext ctx, Class clazz) {
      return clazz.equals(String.class) ||
            clazz.equals(Long.class) ||
            clazz.equals(Double.class) ||
            clazz.equals(Float.class) ||
            clazz.equals(Boolean.class) ||
            clazz.equals(byte[].class) ||
            clazz.equals(Byte.class) ||
            clazz.equals(Short.class) ||
            clazz.equals(Character.class) ||
            clazz.equals(java.util.Date.class) ||
            clazz.equals(java.time.Instant.class) ||
            ctx.canMarshall(clazz);
   }
}
