package org.infinispan.server.core.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.server.core.dataconversion.deserializer.Deserializer;
import org.infinispan.server.core.dataconversion.deserializer.SEntity;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

/**
 * @since 9.2
 */
public class JsonTranscoder extends OneToManyTranscoder {

   protected static final Log logger = LogFactory.getLog(JsonTranscoder.class);

   public static final String TYPE_PROPERTY = "_type";

   private static final JsonFactory JSON_FACTORY = new JsonFactory();

   public JsonTranscoder() {
      super(APPLICATION_JSON, APPLICATION_OCTET_STREAM, APPLICATION_SERIALIZED_OBJECT, TEXT_PLAIN, APPLICATION_WWW_FORM_URLENCODED, APPLICATION_UNKNOWN);
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(APPLICATION_OCTET_STREAM) || destinationType.match(APPLICATION_UNKNOWN)) {
         if (content instanceof byte[]) return content;
         return content.toString().getBytes(contentType.getCharset());
      }
      boolean outputString = destinationType.hasStringType();
      Charset contentCharset = contentType.getCharset();
      Charset destinationCharset = destinationType.getCharset();
      if (destinationType.match(APPLICATION_JSON)) {
         if (contentType.match(APPLICATION_JSON)) {
            return convertCharset(content, contentCharset, destinationCharset, outputString);
         }
         try {
            if (contentType.match(APPLICATION_SERIALIZED_OBJECT) && content instanceof byte[]) {
               return convertJavaSerializedToJson((byte[]) content, destinationCharset, outputString);
            } else if (content instanceof String || content instanceof byte[]) {
               return convertTextToJson(content, contentCharset, destinationCharset, outputString);
            }
         } catch (IOException e) {
            throw logger.errorConvertingContent(content, contentType, destinationType, e);
         }
      }
      if (destinationType.match(TEXT_PLAIN)) {
         return convertCharset(content, contentCharset, destinationCharset, outputString);
      }
      throw logger.cannotConvertContent(JsonTranscoder.class.getSimpleName(), content, contentType, destinationType);
   }

   private Object convertJavaSerializedToJson(byte[] content, Charset destinationCharset, boolean outputAsString) {
      try {
         Deserializer deserializer = new Deserializer(new ByteArrayInputStream(content), true);
         SEntity entity = deserializer.readObject();
         String json = entity.json().toString();
         return outputAsString ? json : StandardConversions.convertCharset(json, java.nio.charset.StandardCharsets.UTF_8, destinationCharset);
      } catch (IOException e) {
         throw logger.errorConvertingContent(content, APPLICATION_SERIALIZED_OBJECT, APPLICATION_JSON, e);
      }
   }

   private Object convertTextToJson(Object content, Charset contentCharset, Charset destinationCharset, boolean asString) throws IOException {
      byte[] bytes = content instanceof byte[] ? (byte[]) content : content.toString().getBytes(contentCharset);
      if (bytes.length == 0) return bytes;
      if (isValidJson(bytes, contentCharset)) {
         return convertCharset(bytes, contentCharset, destinationCharset, asString);
      } else {
         throw logger.invalidJson(new String(bytes));
      }
   }


   public static boolean isValidJson(byte[] content, Charset charset) {
      try (InputStreamReader isr = new InputStreamReader(new ByteArrayInputStream(content), charset);
           BufferedReader reader = new BufferedReader(isr);
           JsonParser parser = JSON_FACTORY.createParser(reader)) {
         parser.nextToken();
         while (parser.hasCurrentToken()) parser.nextToken();
      } catch (IOException e) {
         return false;
      }
      return true;
   }

   private Object convertCharset(Object content, Charset contentCharset, Charset destinationCharset, boolean outputAsString) {
      byte[] bytes = StandardConversions.convertCharset(content, contentCharset, destinationCharset);
      return outputAsString ? new String(bytes, destinationCharset) : bytes;
   }

}
