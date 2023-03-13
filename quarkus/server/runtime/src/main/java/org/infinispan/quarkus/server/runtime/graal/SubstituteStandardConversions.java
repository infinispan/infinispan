package org.infinispan.quarkus.server.runtime.graal;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.StandardConversions;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

@TargetClass(StandardConversions.class)
final class SubstituteStandardConversions {
   // Method substituted so it doesn't have to use Class.forName for well known types - uses switch instead
   @Substitute
   public static Object decodeObjectContent(Object content, MediaType contentMediaType) {
      if (content == null) return null;
      if (contentMediaType == null) {
         throw new NullPointerException("contentMediaType cannot be null!");
      }
      String strContent;
      String type = contentMediaType.getClassType();
      if (type == null) return content;

      if (type.equals("ByteArray")) {
         if (content instanceof byte[]) return content;
         if (content instanceof String) return hexToBytes(content.toString());
         throw new EncodingException("Cannot read ByteArray!");
      }

      if (content instanceof byte[]) {
         strContent = new String((byte[]) content, UTF_8);
      } else {
         strContent = content.toString();
      }

      switch (type) {
         case "java.lang.String":
            return strContent;
         case "java.lang.Boolean":
            return Boolean.parseBoolean(strContent);
         case "java.lang.Short":
            return Short.parseShort(strContent);
         case "java.lang.Byte":
            return Byte.parseByte(strContent);
         case "java.lang.Integer":
            return Integer.parseInt(strContent);
         case "java.lang.Long":
            return Long.parseLong(strContent);
         case "java.lang.Float":
            return Float.parseFloat(strContent);
         case "java.lang.Double":
            return Double.parseDouble(strContent);
      }

      return content;
   }

   @Alias
   public static byte[] hexToBytes(String hex) {
      return null;
   }
}
