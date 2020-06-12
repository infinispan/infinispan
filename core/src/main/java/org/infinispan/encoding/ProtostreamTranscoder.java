package org.infinispan.encoding;

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

   private ImmutableSerializationContext ctx() {
      return ctxRegistry.getGlobalCtx();
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      try {
         if (destinationType.match(MediaType.APPLICATION_PROTOSTREAM)) {
            Object decoded = content;
            if (contentType.match(APPLICATION_OBJECT)) {
               decoded = StandardConversions.decodeObjectContent(content, contentType);
               return marshall(decoded, destinationType);
            }
            if (contentType.match(APPLICATION_OCTET_STREAM)) {
               decoded = decodeOctetStream(content, destinationType);
            }
            if (contentType.match(TEXT_PLAIN)) {
               decoded = convertTextToObject(content, contentType);
            }
            if (contentType.match(APPLICATION_JSON)) {
               Reader reader;
               content = addTypeIfNeeded(content);
               if (content instanceof byte[]) {
                  reader = new InputStreamReader(new ByteArrayInputStream((byte[]) content));
               } else {
                  reader = new StringReader(content.toString());
               }
               return ProtobufUtil.fromCanonicalJSON(ctx(), reader);
            }
            if (contentType.match(APPLICATION_UNKNOWN) || contentType.match(APPLICATION_PROTOSTREAM)) {
               return content;
            }
            return marshall(decoded, destinationType);
         }
         if (destinationType.match(MediaType.APPLICATION_OCTET_STREAM)) {
            Object unmarshalled = content instanceof byte[] ? unmarshall((byte[]) content, contentType, destinationType) : content;
            if (unmarshalled instanceof byte[]) {
               return unmarshalled;
            }
            return StandardConversions.convertJavaToProtoStream(unmarshalled, MediaType.APPLICATION_OBJECT, ctx());
         }
         if (destinationType.match(MediaType.TEXT_PLAIN)) {
            Object decoded = ProtobufUtil.fromWrappedByteArray(ctx(), (byte[]) content);
            if (decoded == null) return null;
            return decoded.toString().getBytes(destinationType.getCharset());
         }
         if (destinationType.match(MediaType.APPLICATION_OBJECT)) {
            return unmarshall((byte[]) content, contentType, destinationType);
         }
         if (destinationType.match(MediaType.APPLICATION_JSON)) {
            String converted = ProtobufUtil.toCanonicalJSON(ctx(), (byte[]) content);
            String convertType = destinationType.getClassType();
            if (convertType == null)
               return StandardConversions.convertCharset(converted, contentType.getCharset(), destinationType.getCharset());
            if (destinationType.hasStringType())
               return converted;
         }
         if (destinationType.equals(APPLICATION_UNKNOWN)) {
            //TODO: Remove wrapping of byte[] into WrappedByteArray from the Hot Rod Multimap operations.
            if (content instanceof WrappedByteArray) return content;
            return StandardConversions.convertJavaToProtoStream(content, MediaType.APPLICATION_OBJECT, ctx());
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
      try {
         if (isWrapped(destinationType)) return ProtobufUtil.toWrappedByteArray(ctx(), decoded);
         return ProtobufUtil.toByteArray(ctx(), decoded);
      } catch (IllegalArgumentException iae) {
         throw new MarshallingException(iae.getMessage());
      }
   }

   private Object unmarshall(byte[] bytes, MediaType contentType, MediaType destinationType) throws IOException {
      try {
         String type = destinationType.getClassType();
         boolean wrapped = isWrapped(contentType);
         if (type == null) {
            if(wrapped) return ProtobufUtil.fromWrappedByteArray(ctx(), bytes);
            throw logger.missingTypeForUnwrappedPayload();
         }
         Class<?> destination = Util.loadClass(type, classLoader);
         return ProtobufUtil.fromByteArray(ctx(), bytes, destination);
      } catch (IllegalArgumentException iae) {
         throw new MarshallingException(iae.getMessage());
      }
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
