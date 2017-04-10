package org.infinispan.rest.server.operations.mediatypes;

import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class Charset {

   public static final Charset UTF8 = new Charset("UTF-8");

   private static final String CHARSET_HEADER = "charset=";

   protected final static Log logger = LogFactory.getLog(Charset.class, Log.class);

   private final java.nio.charset.Charset javaCharset;
   private final String charset;

   private Charset(String charset) {
      javaCharset = java.nio.charset.Charset.forName(charset);
      this.charset = charset;
   }

   public static Charset fromMediaType(String mediaType) {
      int indexOfCharset = mediaType.indexOf(CHARSET_HEADER);
      if (indexOfCharset != -1) {
         try {
            return new Charset(mediaType.substring(indexOfCharset + CHARSET_HEADER.length()));
         } catch (Exception e) {
            logger.trace("Unrecognized charset from media type " + mediaType, e);
         }
      }
      return null;
   }

   public java.nio.charset.Charset getJavaCharset() {
      return javaCharset;
   }

   @Override
   public String toString() {
      return CHARSET_HEADER + charset;
   }
}
