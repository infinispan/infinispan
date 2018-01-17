package org.infinispan.rest.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.rest.JSONConstants.TYPE;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Optional;

import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.OneToManyTranscoder;
import org.infinispan.commons.dataconversion.StandardConversions;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @since 9.2
 */
public class JsonTranscoder extends OneToManyTranscoder {

   protected final static Log logger = LogFactory.getLog(JsonTranscoder.class, Log.class);

   private final ObjectMapper jsonMapper = new ObjectMapper().setDefaultTyping(
         new ObjectMapper.DefaultTypeResolverBuilder(ObjectMapper.DefaultTyping.NON_FINAL) {
            {
               init(JsonTypeInfo.Id.CLASS, null);
               inclusion(JsonTypeInfo.As.PROPERTY);
               typeProperty(TYPE);
            }

            @Override
            public boolean useForType(JavaType t) {
               return !t.isContainerType() && super.useForType(t);
            }
         });

   public JsonTranscoder() {
      super(APPLICATION_JSON, APPLICATION_OBJECT, APPLICATION_OCTET_STREAM, TEXT_PLAIN);
   }

   @Override
   public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
      if (destinationType.match(APPLICATION_OCTET_STREAM)) {
         return content.toString().getBytes(contentType.getCharset());
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
               String contentAsString = new String((byte[]) content, destinationType.getCharset());
               return jsonMapper.writeValueAsString(contentAsString);
            }
            return jsonMapper.writeValueAsString(content);
         } catch (IOException e) {
            throw new CacheException(e);
         }
      }
      if (destinationType.match(APPLICATION_OBJECT)) {
         try {
            if (content instanceof byte[]) {
               return jsonMapper.readValue((byte[]) content, Object.class);
            }
            return jsonMapper.readValue((String) content, Object.class);
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
