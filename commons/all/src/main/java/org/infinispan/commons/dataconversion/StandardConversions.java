package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.commons.logging.Log.CONTAINER;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufUtil;

/**
 * Utilities to convert between text/plain, octet-stream, java-objects and url-encoded contents.
 *
 * @since 9.2
 */
public final class StandardConversions {

   /**
    * Convert text content to a different encoding.
    *
    * @param source The source content.
    * @param sourceType MediaType for the source content.
    * @param destinationType the MediaType of the converted content.
    * @return content conforming to the destination MediaType.
    */
   public static Object convertTextToText(Object source, MediaType sourceType, MediaType destinationType) {
      if (source == null) return null;
      if (sourceType == null) throw new NullPointerException("MediaType cannot be null!");
      if (!sourceType.match(MediaType.TEXT_PLAIN))
         throw CONTAINER.invalidMediaType(TEXT_PLAIN_TYPE, sourceType.toString());

      boolean asString = destinationType.hasStringType();

      Charset sourceCharset = sourceType.getCharset();
      Charset destinationCharset = destinationType.getCharset();
      if (sourceCharset.equals(destinationCharset)) return convertTextClass(source, destinationType, asString);
      byte[] byteContent = source instanceof byte[] ? (byte[]) source : source.toString().getBytes(sourceCharset);
      return convertTextClass(convertCharset(byteContent, sourceCharset, destinationCharset), destinationType, asString);
   }

   private static Object convertTextClass(Object text, MediaType destination, boolean asString) {
      if (asString) {
         return text instanceof byte[] ? new String((byte[]) text, destination.getCharset()) : text.toString();
      }
      return text instanceof byte[] ? text : text.toString().getBytes(destination.getCharset());
   }

   /**
    * Converts text content to the Java representation (String).
    *
    * @param source The source content
    * @param sourceType the MediaType of the source content.
    * @return String representation of the text content.
    * @throws EncodingException if the source cannot be interpreted as plain text.
    */
   public static String convertTextToObject(Object source, MediaType sourceType) {
      if (source == null) return null;
      if (source instanceof String) return source.toString();
      if (source instanceof byte[] bytesSource) {
         return new String(bytesSource, sourceType.getCharset());
      }
      throw CONTAINER.invalidTextContent(source);
   }

   /**
    * Converts a java object to a sequence of bytes using a ProtoStream {@link ImmutableSerializationContext}.
    *
    * @param source source the java object to convert.
    * @param sourceMediaType the MediaType matching application/x-application-object describing the source.
    * @return byte[] representation of the java object.
    * @throws EncodingException if the sourceMediaType is not a application/x-java-object or if the conversion is
    * not supported.
    */
   public static byte[] convertJavaToProtoStream(Object source, MediaType sourceMediaType, ImmutableSerializationContext ctx) throws IOException, InterruptedException {
      if (source == null) return null;
      if (!sourceMediaType.match(MediaType.APPLICATION_OBJECT)) {
         throw new EncodingException("sourceMediaType not conforming to application/x-java-object!");
      }
      if (source instanceof byte[]) return (byte[]) source;
      if (source instanceof String && isJavaString(sourceMediaType))
         return ((String) source).getBytes(StandardCharsets.UTF_8);
      return ProtobufUtil.toWrappedByteArray(ctx, source);
   }

   private static boolean isJavaString(MediaType mediaType) {
      return mediaType.match(MediaType.APPLICATION_OBJECT) && mediaType.hasStringType();
   }


   /**
    * Convert text content.
    *
    * @param content Object to convert.
    * @param fromCharset Charset of the provided content.
    * @param toCharset Charset to convert to.
    * @return byte[] with the content in the desired charset.
    */
   public static byte[] convertCharset(Object content, Charset fromCharset, Charset toCharset) {
      if (content == null) return null;
      if (fromCharset == null || toCharset == null) {
         throw new NullPointerException("Charset cannot be null!");
      }
      byte[] bytes;
      if (content instanceof String) {
         bytes = content.toString().getBytes(fromCharset);
      } else if (content instanceof byte[]) {
         bytes = (byte[]) content;
      } else {
         bytes = content.toString().getBytes(fromCharset);
      }
      if (fromCharset.equals(toCharset)) return bytes;
      CharBuffer inputContent = fromCharset.decode(ByteBuffer.wrap(bytes));
      ByteBuffer result = toCharset.encode(inputContent);
      return Arrays.copyOf(result.array(), result.limit());
   }

}
