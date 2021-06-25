package org.infinispan.encoding;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Optional;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 *<p>
 * Performs conversions between <b>application/x-protostream</b> and commons formats.
 *</p>
 *
 * <br/><p>When converting to <b>application/x-protostream</b>, it will produce payloads
 * with {@link org.infinispan.protostream.WrappedMessage} by default, unless the param
 * <b>wrapped</b> is supplied in the destination {@link MediaType} with value <b>false</b></p>
 *<br/><p>
 * Converting back to <b>application/x-java-object</b> requires either a payload that is
 * a {@link org.infinispan.protostream.WrappedMessage} or an unwrapped payload plus the
 * type of the java object to convert to, specified using the <b>type</b> parameter in
 * the <b>application/x-java-object</b> {@link MediaType}.
 *</p>
 *
 * @since 10.0
 */
public class ProtostreamTranscoder extends OneToManyTranscoder {

   public static final String WRAPPED_PARAM = "wrapped";
   protected static final Log logger = LogFactory.getLog(ProtostreamTranscoder.class, Log.class);
   private final SerializationContextRegistry ctxRegistry;
   private final ClassLoader classLoader;

   public ProtostreamTranscoder(SerializationContextRegistry ctxRegistry, ClassLoader classLoader) {
      super(APPLICATION_PROTOSTREAM, APPLICATION_OCTET_STREAM, TEXT_PLAIN, APPLICATION_OBJECT, APPLICATION_JSON, APPLICATION_UNKNOWN);
      this.ctxRegistry = ctxRegistry;
      this.classLoader = classLoader;
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(MediaType.APPLICATION_PROTOSTREAM)) {
            if (contentType.match(APPLICATION_JSON)) {
               content = addTypeIfNeeded(content);
               return fromJsonCascading(content);
            }
            if (contentType.match(APPLICATION_UNKNOWN) || contentType.match(APPLICATION_PROTOSTREAM)) {
               return content;
            }
            if (contentType.match(TEXT_PLAIN)) {
               content = StandardConversions.convertTextToObject(content, contentType);
            }
            return marshall(content, destinationType);
         }
         if (destinationType.match(MediaType.APPLICATION_OCTET_STREAM)) {
            Object unmarshalled = content instanceof byte[] ? unmarshall((byte[]) content, contentType, destinationType) : content;
            if (unmarshalled instanceof byte[]) {
               return unmarshalled;
            }
            ImmutableSerializationContext ctx = getCtxForMarshalling(unmarshalled);
            return StandardConversions.convertJavaToProtoStream(unmarshalled, MediaType.APPLICATION_OBJECT, ctx);
         }
         if (destinationType.match(MediaType.TEXT_PLAIN)) {
            Object decoded = unmarshallCascading((byte[]) content);
            if (decoded == null) return null;
            return decoded.toString().getBytes(destinationType.getCharset());
         }
         if (destinationType.match(MediaType.APPLICATION_OBJECT)) {
            return unmarshall((byte[]) content, contentType, destinationType);
         }
         if (destinationType.match(MediaType.APPLICATION_JSON)) {
            String converted = toJsonCascading((byte[]) content);
            String convertType = destinationType.getClassType();
            return convertType == null ? StandardConversions.convertCharset(converted, contentType.getCharset(), destinationType.getCharset()) : converted;
         }
         if (destinationType.equals(APPLICATION_UNKNOWN)) {
            //TODO: Remove wrapping of byte[] into WrappedByteArray from the Hot Rod Multimap operations.
            if (content instanceof WrappedByteArray) return content;
            ImmutableSerializationContext ctx = getCtxForMarshalling(content);
            return StandardConversions.convertJavaToProtoStream(content, MediaType.APPLICATION_OBJECT, ctx);
         }
         throw logger.unsupportedContent(ProtostreamTranscoder.class.getSimpleName(), content);
      } catch (InterruptedException | IOException e) {
         throw logger.errorTranscoding(ProtostreamTranscoder.class.getSimpleName(), e);
      }
   }

   private boolean isWrapped(MediaType mediaType) {
      Optional<String> wrappedParam = mediaType.getParameter("wrapped");
      return (!wrappedParam.isPresent() || !wrappedParam.get().equals("false"));
   }

   private byte[] marshall(Object decoded, MediaType destinationType) throws IOException {
      ImmutableSerializationContext ctx = getCtxForMarshalling(decoded);
      if (isWrapped(destinationType)) {
         return ProtobufUtil.toWrappedByteArray(ctx, decoded);
      }
      return ProtobufUtil.toByteArray(ctx, decoded);
   }

   private Object unmarshall(byte[] bytes, MediaType contentType, MediaType destinationType) throws IOException {
      if (isWrapped(contentType))
         return unmarshallCascading(bytes);

      String type = destinationType.getClassType();
      if (type == null) throw logger.missingTypeForUnwrappedPayload();
      Class<?> destination = Util.loadClass(type, classLoader);
      ImmutableSerializationContext ctx = getCtxForMarshalling(destination);
      return ProtobufUtil.fromByteArray(ctx, bytes, destination);
   }

   // Workaround until protostream provides support for cascading contexts IPROTO-139
   private Object unmarshallCascading(byte[] bytes) throws IOException {
      // First try to unmarshalling with the user context
      try {
         return ProtobufUtil.fromWrappedByteArray(ctxRegistry.getUserCtx(), bytes);
      } catch (IllegalArgumentException e) {
         logger.debugf("Unable to unmarshall bytes with user context, attempting global context");
         try {
            return ProtobufUtil.fromWrappedByteArray(ctxRegistry.getGlobalCtx(), bytes);
         } catch (IllegalArgumentException iae) {
            throw new MarshallingException(iae.getMessage());
         }
      }
   }

   // Workaround until protostream provides support for cascading contexts IPROTO-139
   private byte[] fromJsonCascading(Object content) throws IOException {
      try {
         return fromJson(content, ctxRegistry.getUserCtx());
      } catch (IllegalArgumentException e) {
         logger.debugf("Unable to process json with user context, attempting global context");
         return fromJson(content, ctxRegistry.getGlobalCtx());
      }
   }

   private byte[] fromJson(Object content, ImmutableSerializationContext ctx) throws IOException {
      Reader reader;
      if (content instanceof byte[]) {
         reader = new InputStreamReader(new ByteArrayInputStream((byte[]) content));
      } else {
         reader = new StringReader(content.toString());
      }
      return ProtobufUtil.fromCanonicalJSON(ctx, reader);
   }

   // Workaround until protostream provides support for cascading contexts IPROTO-139
   private String toJsonCascading(byte[] bytes) throws IOException {
      try {
         return ProtobufUtil.toCanonicalJSON(ctxRegistry.getUserCtx(), bytes);
      } catch (IllegalArgumentException e) {
         logger.debugf("Unable to read bytes with user context, attempting global context");
         return ProtobufUtil.toCanonicalJSON(ctxRegistry.getGlobalCtx(), bytes);
      }
   }

   private ImmutableSerializationContext getCtxForMarshalling(Object o) {
      Class<?> clazz = o instanceof Class<?> ? (Class<?>) o : o.getClass();
      if (isWrappedMessageClass(clazz) || ctxRegistry.getUserCtx().canMarshall(clazz))
         return ctxRegistry.getUserCtx();

      if (ctxRegistry.getGlobalCtx().canMarshall(clazz))
         return ctxRegistry.getGlobalCtx();

      throw logger.marshallerMissingFromUserAndGlobalContext(o.getClass().getName());
   }

   private boolean isWrappedMessageClass(Class<?> c) {
      return c.equals(String.class) ||
            c.equals(Long.class) ||
            c.equals(Integer.class) ||
            c.equals(Double.class) ||
            c.equals(Float.class) ||
            c.equals(Boolean.class) ||
            c.equals(byte[].class) ||
            c.equals(Byte.class) ||
            c.equals(Short.class) ||
            c.equals(Character.class) ||
            c.equals(java.util.Date.class) ||
            c.equals(java.time.Instant.class);
   }

   private Object addTypeIfNeeded(Object content) {
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
}
