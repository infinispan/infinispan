package org.infinispan.commons.dataconversion;

import static java.util.function.Function.identity;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @since 13.0
 */
enum JavaStringCodec {
   BYTE_ARRAY("ByteArray") {
      @Override
      Object decode(String strContent) {
         return Base16Codec.decode(strContent);
      }

      @Override
      Object encode(Object content, MediaType destinationType) {
         if (content instanceof byte[]) {
            return Base16Codec.encode((byte[]) content);
         }
         if (content instanceof String) {
            return content.toString().getBytes(destinationType.getCharset());
         }
         throw new EncodingException("Cannot encode " + content.getClass() + " as ByteArray");
      }
   },
   INTEGER(Integer.class.getName()) {
      @Override
      Object decode(String strContent) {
         return Integer.parseInt(strContent);
      }

      @Override
      Object encode(Object content, MediaType destinationType) {
         return Integer.valueOf(content.toString()).toString();
      }
   },
   STRING(String.class.getName()) {
      @Override
      Object decode(String strContent) {
         return strContent;
      }

      @Override
      Object encode(Object content, MediaType destinationType) {
         if (content instanceof byte[]) {
            return new String((byte[]) content, destinationType.getCharset());
         } else if (content instanceof String) {
            return content;
         }
         throw new EncodingException("Cannot encode " + content.getClass() + " as String");
      }
   },
   BOOLEAN(Boolean.class.getName()) {
      @Override
      Object decode(String strContent) {
         return Boolean.parseBoolean(strContent);
      }

      @Override
      Object encode(Object content, MediaType destinationType) {
         return Boolean.valueOf(content.toString()).toString();
      }
   },
   SHORT(Short.class.getName()) {
      @Override
      Object decode(String strContent) {
         return Short.parseShort(strContent);
      }

      @Override
      Object encode(Object content, MediaType destinationType) {
         return Short.valueOf(content.toString()).toString();
      }
   },
   BYTE(Byte.class.getName()) {
      @Override
      Object decode(String strContent) {
         return Byte.parseByte(strContent);
      }

      @Override
      Object encode(Object content, MediaType destinationType) {
         return Byte.valueOf(content.toString()).toString();
      }
   },
   LONG(Long.class.getName()) {
      @Override
      Object decode(String strContent) {
         return Long.parseLong(strContent);
      }

      @Override
      Object encode(Object content, MediaType destinationType) {
         return Long.valueOf(content.toString()).toString();
      }
   },
   FLOAT(Float.class.getName()) {
      @Override
      Object decode(String strContent) {
         return Float.parseFloat(strContent);
      }

      @Override
      Object encode(Object content, MediaType destinationType) {
         return Float.valueOf(content.toString()).toString();
      }
   },
   DOUBLE(Double.class.getName()) {
      @Override
      Object decode(String strContent) {
         return Double.parseDouble(strContent);
      }

      @Override
      Object encode(Object content, MediaType destinationType) {
         return Double.valueOf(content.toString()).toString();
      }
   };

   private final String name;

   private static final Map<String, JavaStringCodec> CACHE = Arrays.stream(JavaStringCodec.values())
         .collect(Collectors.toMap(JavaStringCodec::getName, identity()));

   JavaStringCodec(String name) {
      this.name = name;
   }

   public String getName() {
      return name;
   }

   abstract Object decode(String str);

   abstract Object encode(Object value, MediaType destinationType);

   static JavaStringCodec forType(String type) {
      return CACHE.get(type);
   }
}
