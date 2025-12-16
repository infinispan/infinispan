package org.infinispan.scripting.utils;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;

import java.util.Collection;
import java.util.Collections;
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

   public static final MediaType APPLICATION_TEXT_STRING = MediaType.TEXT_PLAIN.withClassType(String.class);

   private final Map<String, OutputFormatter> formatterByMediaType = new HashMap<>(2);
   private final EncoderRegistry encoderRegistry;

   public ScriptConversions(EncoderRegistry encoderRegistry) {
      this.encoderRegistry = encoderRegistry;
      formatterByMediaType.put(TEXT_PLAIN_TYPE, new TextPlainFormatter());
   }

   public Map<String, Object> convertParameters(TaskContext context) {
      if (context.getParameters().isEmpty()) return null;
      Map<String, Object> contextParams = context.getParameters().get();
      if (contextParams == Collections.EMPTY_MAP) {
         return new HashMap<>(2);
      }
      Map<String, Object> converted = new HashMap<>(contextParams.size());

      if (context.getCache().isPresent()) {
         DataConversion valueDataConversion = context.getCache().get().getAdvancedCache().getValueDataConversion();
         MediaType requestMediaType = valueDataConversion.getRequestMediaType();
         contextParams.forEach((s, o) -> {
            Object c = requestMediaType == null ? o : encoderRegistry.convert(o, valueDataConversion.getRequestMediaType(), APPLICATION_OBJECT);
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
      // Older HR clients do not send request type and assume the script metadata type is the output type
      MediaType outputFormat = requestType.match(MediaType.APPLICATION_UNKNOWN) ? objType : requestType;
      OutputFormatter outputFormatter = formatterByMediaType.get(outputFormat.getTypeSubtype());
      if (obj instanceof Collection) {
         if (outputFormatter != null) {
            return outputFormatter.formatCollection((Collection<?>) obj, objType, requestType);
         }
      }
      Transcoder transcoder = encoderRegistry.getTranscoder(objType, requestType);
      return transcoder.transcode(obj, objType, requestType);
   }

   private interface OutputFormatter {
      Object formatCollection(Collection<?> elements, MediaType elementType, MediaType destinationType);
   }

   private class TextPlainFormatter implements OutputFormatter {

      private String quote(Object element) {
         if (element == null) return "null";
         return "\"" + element.toString() + "\"";
      }

      @Override
      public Object formatCollection(Collection<?> elements, MediaType elementType, MediaType destinationType) {
         Transcoder transcoder = encoderRegistry.getTranscoder(elementType, APPLICATION_TEXT_STRING);

         return elements.stream().map(s -> transcoder.transcode(s, elementType, APPLICATION_TEXT_STRING))
               .map(this::quote).collect(Collectors.joining(", ", "[", "]"))
               .getBytes(destinationType.getCharset());
      }
   }
}
