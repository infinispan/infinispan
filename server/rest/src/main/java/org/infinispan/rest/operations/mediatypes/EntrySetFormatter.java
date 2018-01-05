package org.infinispan.rest.operations.mediatypes;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.IMAGE_PNG_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.MATCH_ALL_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.operations.mediatypes.impl.BinaryOutputPrinter;
import org.infinispan.rest.operations.mediatypes.impl.JSONOutputPrinter;
import org.infinispan.rest.operations.mediatypes.impl.TextOutputPrinter;
import org.infinispan.rest.operations.mediatypes.impl.XMLOutputPrinter;

/**
 * Formats collections of entries based on the associated {@link MediaType};
 *
 */
public class EntrySetFormatter {

   private static Map<String, OutputPrinter> printerByMediaType = new HashMap<>(10);

   static {
      TextOutputPrinter textOutputPrinter = new TextOutputPrinter();
      XMLOutputPrinter xmlOutputPrinter = new XMLOutputPrinter();
      JSONOutputPrinter jsonOutputPrinter = new JSONOutputPrinter();
      BinaryOutputPrinter binaryOutputPrinter = new BinaryOutputPrinter();

      printerByMediaType.put(TEXT_PLAIN_TYPE, textOutputPrinter);
      printerByMediaType.put(APPLICATION_XML_TYPE, xmlOutputPrinter);
      printerByMediaType.put(APPLICATION_JSON_TYPE, jsonOutputPrinter);
      printerByMediaType.put(APPLICATION_OCTET_STREAM_TYPE, jsonOutputPrinter);
      printerByMediaType.put(IMAGE_PNG_TYPE, binaryOutputPrinter);
      printerByMediaType.put(APPLICATION_SERIALIZED_OBJECT_TYPE, binaryOutputPrinter);
      printerByMediaType.put(MATCH_ALL_TYPE, textOutputPrinter);
   }

   public static OutputPrinter forMediaType(MediaType mediaType) {
      return printerByMediaType.get(mediaType.getTypeSubtype());
   }

}
