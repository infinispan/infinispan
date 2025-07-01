package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JBOSS_MARSHALLING;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PDF;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_RTF;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_ZIP;
import static org.infinispan.commons.dataconversion.MediaType.IMAGE_GIF;
import static org.infinispan.commons.dataconversion.MediaType.IMAGE_JPEG;
import static org.infinispan.commons.dataconversion.MediaType.IMAGE_PNG;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_CSS;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_CSV;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_HTML;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;

import java.util.HashMap;
import java.util.Map;

/**
 * Short ids for common media types
 *
 * @since 9.2
 */
public final class MediaTypeIds {

   private static final Map<MediaType, Short> idByType = new HashMap<>(16);
   private static final Map<Short, MediaType> typeById = new HashMap<>(16);

   static {
      idByType.put(APPLICATION_OBJECT, (short) 1);
      idByType.put(APPLICATION_JSON, (short) 2);
      idByType.put(APPLICATION_OCTET_STREAM, (short) 3);
      idByType.put(APPLICATION_PDF, (short) 4);
      idByType.put(APPLICATION_RTF, (short) 5);
      idByType.put(APPLICATION_ZIP, (short) 6);
      idByType.put(IMAGE_GIF, (short) 7);
      idByType.put(IMAGE_JPEG, (short) 8);
      idByType.put(IMAGE_PNG, (short) 9);
      idByType.put(TEXT_CSS, (short) 10);
      idByType.put(TEXT_CSV, (short) 11);
      idByType.put(APPLICATION_PROTOSTREAM, (short) 12);
      idByType.put(TEXT_PLAIN, (short) 13);
      idByType.put(TEXT_HTML, (short) 14);
      idByType.put(APPLICATION_JBOSS_MARSHALLING, (short) 15);
      idByType.put(APPLICATION_UNKNOWN, (short) 17);

      idByType.forEach((key, value) -> typeById.put(value, key));
   }

   public static Short getId(MediaType mediaType) {
      if (mediaType == null) return null;
      return idByType.get(mediaType.withoutParameters());
   }

   public static MediaType getMediaType(Short id) {
      return typeById.get(id);
   }

}
