package org.infinispan.commons.dataconversion;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;

/**
 * Resolve media types for files
 *
 * @since 10.1
 */
public final class MediaTypeResolver {

   private static final Map<String, String> FILE_MAP = new HashMap<>();
   private static final Log LOG = LogFactory.getLog(MediaTypeResolver.class);
   private static final String MIME_TYPES = "mime.types";

   static {
      InputStream in = null;
      BufferedInputStream bis = null;
      try {
         in = MediaTypeResolver.class.getClassLoader().getResourceAsStream(MIME_TYPES);
         if (in == null) {
            LOG.cannotLoadMimeTypes(MIME_TYPES);
         } else {
            bis = new BufferedInputStream(in);
            Scanner scanner = new Scanner(bis);
            while (scanner.hasNextLine()) {
               String line = scanner.nextLine();
               if (!line.startsWith("#")) {
                  String[] split = line.split("\\s+");
                  if (split.length > 1) {
                     String mediaType = split[0];
                     for (int i = 1; i < split.length; i++) FILE_MAP.put(split[i], mediaType);
                  }
               }
            }
         }
      } finally {
         Util.close(in, bis);
         LOG.debugf("Loaded %s with %d file types", MIME_TYPES, FILE_MAP.size());
      }
   }

   private MediaTypeResolver() {
   }

   /**
    * @param fileName The file name
    * @return The media type based on the internal mime.types file, or null if not found.
    */
   public static String getMediaType(String fileName) {
      if (fileName == null) return null;
      int idx = fileName.lastIndexOf(".");
      if (idx == -1 || idx == fileName.length()) return null;
      return FILE_MAP.get(fileName.toLowerCase().substring(idx + 1));
   }
}
