package org.infinispan.transcoder.json;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collections;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.DefaultBaseTypeLimitingValidator;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Transcoder that converts Java objects (APPLICATION_OBJECT) to JSON using Jackson databind.
 *
 * @since 16.3
 */
public class JsonPojoTranscoder extends OneToManyTranscoder {

   private static final Log logger = LogFactory.getLog(JsonPojoTranscoder.class);

   public static final String TYPE_PROPERTY = "_type";

   private final ObjectMapper objectMapper;

   public JsonPojoTranscoder() {
      this(JsonPojoTranscoder.class.getClassLoader(), new ClassAllowList(Collections.emptyList()));
   }

   public JsonPojoTranscoder(ClassAllowList allowList) {
      this(JsonPojoTranscoder.class.getClassLoader(), allowList);
   }

   public JsonPojoTranscoder(ClassLoader classLoader, ClassAllowList allowList) {
      super(APPLICATION_OBJECT, APPLICATION_JSON);
      this.objectMapper = new ObjectMapper();
      TypeFactory typeFactory = TypeFactory.defaultInstance().withClassLoader(classLoader);
      this.objectMapper.setTypeFactory(typeFactory).setDefaultTyping(
            new ObjectMapper.DefaultTypeResolverBuilder(ObjectMapper.DefaultTyping.NON_FINAL, new DefaultBaseTypeLimitingValidator()) {
               {
                  init(JsonTypeInfo.Id.CLASS, null);
                  inclusion(JsonTypeInfo.As.PROPERTY);
                  typeProperty(TYPE_PROPERTY);
               }

               @Override
               public boolean useForType(JavaType t) {
                  if (!t.isContainerType()) {
                     return super.useForType(t);
                  }
                  JavaType contentType = t.getContentType();
                  if (contentType != null && contentType.isFinal() && !contentType.isAbstract() && !contentType.isCollectionLikeType() && !contentType.isMapLikeType()) {
                     return false;
                  }
                  return super.useForType(t);
               }
            }
      );
   }

   @Override
   public Object doTranscode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(APPLICATION_JSON)) {
         if (content instanceof byte[] && ((byte[]) content).length == 0) {
            return content;
         }
         boolean outputString = destinationType.hasStringType();
         Charset destinationCharset = destinationType.getCharset();
         try {
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
            throw logger.errorConvertingContent(content, contentType, destinationType, e);
         }
      }
      throw logger.cannotConvertContent(JsonPojoTranscoder.class.getSimpleName(), content, contentType, destinationType);
   }

}
