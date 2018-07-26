package org.infinispan.scripting.utils;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.encoding.DataConversion;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.tasks.TaskContext;

/**
 * @since 9.4
 */
public final class ScriptConversions {

   public static final MediaType APPLICATION_TEXT_STRING = MediaType.TEXT_PLAIN.withParameter("type", String.class.getName());

   private final Map<String, OutputFormatter> formatterByMediaType = new HashMap<>(2);
   private final EncoderRegistry encoderRegistry;

   public ScriptConversions(EncoderRegistry encoderRegistry) {
      this.encoderRegistry = encoderRegistry;
      formatterByMediaType.put(TEXT_PLAIN_TYPE, new TextPlainFormatter());
   }

   public Map<String, ?> convertParameters(TaskContext context) {
      if (!context.getParameters().isPresent()) return null;
      Map<String, ?> contextParams = context.getParameters().get();
      Map<String, Object> converted = new HashMap<>(contextParams.size());

      if (context.getCache().isPresent()) {
         DataConversion valueDataConversion = context.getCache().get().getAdvancedCache().getValueDataConversion();
         MediaType requestMediaType = valueDataConversion.getRequestMediaType();
         contextParams.forEach((s, o) -> {
            Object c = requestMediaType == null ? o : valueDataConversion.convert(o, valueDataConversion.getRequestMediaType(), APPLICATION_OBJECT);
            converted.put(s, c);
         });
         return converted;
      } else {
         return contextParams;
      }
   }

   public Object convertToRequestType(Object obj, MediaType objType, MediaType requestType) {
      if (obj == null) return null;
      if (requestType.equals(MediaType.MATCH_ALL)) return obj;
      OutputFormatter outputFormatter = formatterByMediaType.get(requestType.getTypeSubtype());
      if (obj instanceof Collection && outputFormatter != null) {
         return outputFormatter.formatCollection((Collection<?>) obj, objType, requestType);
      }
      Transcoder transcoder = encoderRegistry.getTranscoder(objType, requestType);
      return transcoder.transcode(obj, objType, requestType);
   }

   private interface OutputFormatter {
      Object formatCollection(Collection<?> elements, MediaType elementType, MediaType destinationType);
   }

   private class TextPlainFormatter implements OutputFormatter {
      @Override
      public Object formatCollection(Collection<?> elements, MediaType elementType, MediaType destinationType) {
         Transcoder transcoder = encoderRegistry.getTranscoder(elementType, APPLICATION_TEXT_STRING);

         return elements.stream().map(s -> transcoder.transcode(s, elementType, APPLICATION_TEXT_STRING).toString())
               .collect(Collectors.joining("\", \"", "[\"", "\"]"))
               .getBytes(destinationType.getCharset());
      }
   }
}
