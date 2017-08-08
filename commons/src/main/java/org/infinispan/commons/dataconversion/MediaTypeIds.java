package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PDF_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_RTF_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_ZIP_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.IMAGE_GIF_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.IMAGE_JPEG_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.IMAGE_PNG_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_CSS_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_CSV_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_HTML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;

import java.util.HashMap;
import java.util.Map;

/**
 * @since 9.2
 */
final class MediaTypeIds {

   private static final Map<String, Short> idByType = new HashMap<>(32);
   private static final Map<Short, String> typeById = new HashMap<>(32);

   static {
      idByType.put(APPLICATION_OBJECT_TYPE, (short) 1);
      idByType.put(APPLICATION_JSON_TYPE, (short) 2);
      idByType.put(APPLICATION_OCTET_STREAM_TYPE, (short) 3);
      idByType.put(APPLICATION_PDF_TYPE, (short) 4);
      idByType.put(APPLICATION_RTF_TYPE, (short) 5);
      idByType.put(APPLICATION_ZIP_TYPE, (short) 6);
      idByType.put(IMAGE_GIF_TYPE, (short) 7);
      idByType.put(IMAGE_JPEG_TYPE, (short) 8);
      idByType.put(IMAGE_PNG_TYPE, (short) 9);
      idByType.put(TEXT_CSS_TYPE, (short) 10);
      idByType.put(TEXT_CSV_TYPE, (short) 11);
      idByType.put(APPLICATION_PROTOSTREAM_TYPE, (short) 12);
      idByType.put(TEXT_PLAIN_TYPE, (short) 13);
      idByType.put(TEXT_HTML_TYPE, (short) 14);

      idByType.entrySet().forEach(e -> typeById.put(e.getValue(), e.getKey()));
   }

   static Short getId(String mediaType) {
      return idByType.get(mediaType);
   }

   static String getMediaType(Short id) {
      return typeById.get(id);
   }

}
