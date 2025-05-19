package org.infinispan.server.core.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_WWW_FORM_URLENCODED;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.server.core.dataconversion.deserializer.Deserializer;
import org.infinispan.server.core.dataconversion.deserializer.SEntity;
import org.infinispan.server.core.dataconversion.json.SecureTypeResolverBuilder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * @since 9.2
 */
public class JsonTranscoder extends OneToManyTranscoder {

   protected static final Log logger = LogFactory.getLog(JsonTranscoder.class, Log.class);

   public static final String TYPE_PROPERTY = "_type";

   private final ObjectMapper objectMapper;
   private static final JsonFactory JSON_FACTORY = new JsonFactory();

   public JsonTranscoder() {
      this(JsonTranscoder.class.getClassLoader(), new ClassAllowList(Collections.emptyList()));
   }


   public JsonTranscoder(ClassAllowList allowList) {
      this(JsonTranscoder.class.getClassLoader(), allowList);
   }

   public JsonTranscoder(ClassLoader classLoader, ClassAllowList allowList) {
      super(APPLICATION_JSON, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM, APPLICATION_SERIALIZED_OBJECT, TEXT_PLAIN, APPLICATION_WWW_FORM_URLENCODED, APPLICATION_UNKNOWN);
      this.objectMapper = new ObjectMapper().setDefaultTyping(
            new SecureTypeResolverBuilder(ObjectMapper.DefaultTyping.NON_FINAL, allowList) {
               {
                  init(JsonTypeInfo.Id.CLASS, null);
                  inclusion(JsonTypeInfo.As.PROPERTY);
                  typeProperty(TYPE_PROPERTY);
               }

               @Override
               public boolean useForType(JavaType t) {
                  return !t.isContainerType() && super.useForType(t);
               }
            });
      TypeFactory typeFactory = TypeFactory.defaultInstance().withClassLoader(classLoader);
      this.objectMapper.setTypeFactory(typeFactory);
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(APPLICATION_OCTET_STREAM) || destinationType.match(APPLICATION_UNKNOWN)) {
         return StandardConversions.convertTextToOctetStream(content, contentType);
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
            logger.jsonObjectConversionDeprecated();
            if (outputString) {
               return objectMapper.writeValueAsString(content);
            }
            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 OutputStreamWriter osw = new OutputStreamWriter(out, destinationCharset)) {
               objectMapper.writeValue(osw, content);
               return out.toByteArray();
            }

         } catch (IOException e) {
            throw logger.cannotConvertContent(content, contentType, destinationType, e);
         }
      }
      if (destinationType.match(APPLICATION_OBJECT)) {
         logger.jsonObjectConversionDeprecated();
         try {
            String destinationClassName = destinationType.getClassType();
            Class<?> destinationClass = Object.class;
            if (destinationClassName != null) destinationClass = Class.forName(destinationClassName);
            if (content instanceof byte[]) {
               return objectMapper.readValue((byte[]) content, destinationClass);
            }
            return objectMapper.readValue((String) content, destinationClass);
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
      }
      if (destinationType.match(TEXT_PLAIN)) {
         return convertCharset(content, contentCharset, destinationCharset, outputString);
      }
      throw logger.unsupportedContent(JsonTranscoder.class.getSimpleName(), content);
   }

   private Object convertJavaSerializedToJson(byte[] content, Charset destinationCharset, boolean outputAsString) {
      try {
         Deserializer deserializer = new Deserializer(new ByteArrayInputStream(content), true);
         SEntity entity = deserializer.readObject();
         String json = entity.json().toString();
         return outputAsString ? json : StandardConversions.convertCharset(json, StandardCharsets.UTF_8, destinationCharset);
      } catch (IOException e) {
         throw logger.cannotConvertContent(content, APPLICATION_SERIALIZED_OBJECT, APPLICATION_JSON, e);
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
