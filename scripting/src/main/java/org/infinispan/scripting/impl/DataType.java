package org.infinispan.scripting.impl;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Immutables;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public enum DataType {
   UTF8(new Utf8Transformer()),
   DEFAULT(new DefaultTransformer());

   public final Transformer transformer;

   DataType(Transformer transformer) {
      this.transformer = transformer;
   }

   public static DataType fromMime(String mime) {
      switch (mime) {
         case "text/plain; charset=utf-8":
            return UTF8;
         default:
            return DEFAULT;
      }
   }

   interface Transformer {
      Map<String, ?> toDataType(Map<String, ?> objs, Optional<Marshaller> marshaller);
      Object fromDataType(Object obj, Optional<Marshaller> marshaller);
   }

   static final class Utf8Transformer implements Transformer {
      public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");

      @Override
      public Map<String, ?> toDataType(Map<String, ?> objs, Optional<Marshaller> marshaller) {
         return objs.entrySet().stream().map(e -> {
            Object v = e.getValue();
            Object entryValue = v instanceof byte[] ? asString(v) : v;
            return Immutables.immutableEntry(e.getKey(), entryValue);
         }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }

      @Override
      public Object fromDataType(Object obj, Optional<Marshaller> marshaller) {
         return obj instanceof String ? ((String) obj).getBytes(CHARSET_UTF8) : obj;
      }

      private static String asString(Object v) {
         return new String((byte[]) v, CHARSET_UTF8);
      }
   }

   static final class DefaultTransformer implements Transformer {
      @Override
      public Map<String, ?> toDataType(Map<String, ?> objs, Optional<Marshaller> marshaller) {
         if (marshaller.isPresent()) {
            Marshaller m = marshaller.get();
            return objs.entrySet().stream().map(e -> {
               Object v = e.getValue();
               Object entryValue = v instanceof byte[] ? fromBytes(v, m) : v;
               return Immutables.immutableEntry(e.getKey(), entryValue);
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
         }

         return objs;
      }

      private static Object fromBytes(Object obj, Marshaller marshaller) {
         try {
            return marshaller.objectFromByteBuffer((byte[]) obj);
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }

      @Override
      public Object fromDataType(Object obj, Optional<Marshaller> marshaller) {
         try {
            return marshaller.map(m -> toBytes(obj, m)).orElse(obj);
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }

      private static Object toBytes(Object obj, Marshaller marshaller) {
         try {
            return obj instanceof byte[] ? obj : marshaller.objectToByteBuffer(obj);
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }
   }

}
