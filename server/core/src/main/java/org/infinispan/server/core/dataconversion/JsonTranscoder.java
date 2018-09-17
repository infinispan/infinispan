package org.infinispan.server.core.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collections;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.server.core.dataconversion.json.SecureTypeResolverBuilder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * @since 9.2
 */
public class JsonTranscoder extends OneToManyTranscoder {

   protected final static Log logger = LogFactory.getLog(JsonTranscoder.class, Log.class);

   public static final String TYPE_PROPERTY = "_type";

   private final ObjectMapper objectMapper;

   public JsonTranscoder() {
      this(JsonTranscoder.class.getClassLoader(), new ClassWhiteList(Collections.emptyList()));
   }


   public JsonTranscoder(ClassWhiteList whiteList) {
      this(JsonTranscoder.class.getClassLoader(), whiteList);
   }

   public JsonTranscoder(ClassLoader classLoader, ClassWhiteList whiteList) {
      super(APPLICATION_JSON, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM, TEXT_PLAIN, APPLICATION_UNKNOWN);
      this.objectMapper = new ObjectMapper().setDefaultTyping(
            new SecureTypeResolverBuilder(ObjectMapper.DefaultTyping.NON_FINAL, whiteList) {
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
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
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
            if (content instanceof String || content instanceof byte[]) {
               return convertTextToJson(content, contentCharset, destinationCharset, outputString);
            }

            if (outputString) {
               return objectMapper.writeValueAsString(content);
            }
            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 OutputStreamWriter osw = new OutputStreamWriter(out, destinationCharset)) {
               objectMapper.writeValue(osw, content);
               return out.toByteArray();
            }

         } catch (IOException e) {
            throw logger.cannotConvertContent(content, contentType, destinationType);
         }
      }
      if (destinationType.match(APPLICATION_OBJECT)) {
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
      throw logger.unsupportedContent(content);
   }

   private Object convertTextToJson(Object content, Charset contentCharset, Charset destinationCharset, boolean asString) throws IOException {
      byte[] bytes = content instanceof byte[] ? (byte[]) content : content.toString().getBytes(contentCharset);
      if (bytes.length == 0) return bytes;
      try (InputStreamReader isr = new InputStreamReader(new ByteArrayInputStream(bytes), contentCharset)) {
         String jsonTree = objectMapper.readTree(isr).toString();
         return asString ? jsonTree : jsonTree.getBytes(destinationCharset);
      } catch (IOException e) {
         String contentAsString = new String(bytes, contentCharset);
         return asString ? objectMapper.writeValueAsString(contentAsString) : objectMapper.writeValueAsBytes(contentAsString);
      }
   }


   private Object convertCharset(Object content, Charset contentCharset, Charset destinationCharset, boolean outputAsString) {
      byte[] bytes = StandardConversions.convertCharset(content, contentCharset, destinationCharset);
      return outputAsString ? new String(bytes, destinationCharset) : bytes;
   }

}
