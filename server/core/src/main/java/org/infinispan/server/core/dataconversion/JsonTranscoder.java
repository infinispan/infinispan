package org.infinispan.server.core.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Optional;

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
      if (destinationType.match(APPLICATION_JSON)) {
         if (contentType.match(APPLICATION_JSON)) {
            Charset sourceCharset = contentType.getCharset();
            Charset destinationCharset = destinationType.getCharset();
            if (!sourceCharset.equals(destinationCharset)) {
               return StandardConversions.convertCharset(content, sourceCharset, destinationCharset);
            }
            return content;
         }
         try {
            if (content instanceof byte[]) {
               try {
                  return objectMapper.readTree((byte[]) content).toString().getBytes(destinationType.getCharset());
               } catch (IOException e) {
                  String contentAsString = new String((byte[]) content, destinationType.getCharset());
                  return objectMapper.writeValueAsBytes(contentAsString);
               }
            }
            return objectMapper.writeValueAsBytes(content);
         } catch (IOException e) {
            throw logger.cannotConvertContent(content, contentType, destinationType);
         }
      }
      if (destinationType.match(APPLICATION_OBJECT)) {
         try {
            if (content instanceof byte[]) {
               return objectMapper.readValue((byte[]) content, Object.class);
            }
            return objectMapper.readValue((String) content, Object.class);
         } catch (IOException e) {
            throw new CacheException(e);
         }
      }
      if (destinationType.match(TEXT_PLAIN)) {
         Optional<String> optDestinationCharset = destinationType.getParameter("charset");
         if (!optDestinationCharset.isPresent()) return content;
         if (content instanceof byte[]) {
            Charset sourceCharset = contentType.getParameter("charset").map(Charset::forName).orElse(Charset.defaultCharset());
            Charset destinationCharset = Charset.forName(optDestinationCharset.get());

            byte[] byteContent = (byte[]) content;
            return StandardConversions.convertCharset(byteContent, sourceCharset, destinationCharset);
         }
      }
      throw logger.unsupportedContent(content);
   }

}
