package org.infinispan.rest.operations.mediatypes;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Represents Charset.
 *
 * <p>
 *    Charsets are technically an optional part of a {@link MediaType}.
 * </p>
 *
 * @author Sebastian Łaskawiec
 */
public class Charset {

   /**
    * Default {@link Charset} - UTF8.
    */
   public static final Charset UTF8 = new Charset("UTF-8");

   private static final String CHARSET_HEADER = "charset=";

   protected static final Log logger = LogFactory.getLog(Charset.class, Log.class);

   private final java.nio.charset.Charset javaCharset;
   private final String charset;

   private Charset(String charset) {
      javaCharset = java.nio.charset.Charset.forName(charset);
      this.charset = charset;
   }

   /**
    * Creates {@link Charset} based on {@link org.infinispan.commons.dataconversion.MediaType} as string.
    *
    * @param mediaType {@link org.infinispan.commons.dataconversion.MediaType} value.
    * @return Returns {@link Charset} value or <code>null</code> if charset is not supported.
    */
   public static Charset fromMediaType(String mediaType) {
      if (mediaType == null) return null;
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

   /**
    * Creates Java {@link java.nio.charset.Charset} from this object.
    *
    * @return Java {@link java.nio.charset.Charset} from this object.
    */
   public java.nio.charset.Charset getJavaCharset() {
      return javaCharset;
   }

   @Override
   public String toString() {
      return CHARSET_HEADER + charset;
   }
}
