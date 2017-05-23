package org.infinispan.rest.server.operations.mediatypes;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.rest.server.operations.mediatypes.printers.TextOutputPrinter;
import org.infinispan.rest.server.operations.mediatypes.printers.HTMLOutputPrinter;
import org.infinispan.rest.server.operations.mediatypes.printers.JSONOutputPrinter;
import org.infinispan.rest.server.operations.mediatypes.printers.XMLOutputPrinter;

public enum MediaType {

   TEXT_PLAIN("text/plain", new TextOutputPrinter()),
   TEXT_HTML("text/html", new HTMLOutputPrinter()),
   APPLICATION_XML("application/xml", new XMLOutputPrinter()),
   APPLICATION_JSON("application/json", new JSONOutputPrinter());
   APPLICATION_OCTET_STREAM("application/octet-stream", new BinaryOutputPrinter()),
   APPLICATION_BINARY("application/binary", new BinaryOutputPrinter());

   private static final Map<String, MediaType> reverseLookup = new HashMap<>(MediaType.values().length);

   static {
      for(MediaType type : MediaType.values()) {
         reverseLookup.put(type.mediaTypeAsString, type);
      }
   }

   private final String mediaTypeAsString;
   private final OutputPrinter outputPrinter;

   MediaType(String mediaTypeAsString, OutputPrinter outputPrinter) {
      this.mediaTypeAsString = mediaTypeAsString;
      this.outputPrinter = outputPrinter;
   }

   public static MediaType fromMediaTypeAsString(String text) {
      return reverseLookup.get(sanitize(text));
   }

   private static String sanitize(String text) {
      int charsetSeparator = text.indexOf(';');
      if (charsetSeparator != -1) {
         text = text.substring(0, text.indexOf(";"));
      }
      return text;
   }

   public OutputPrinter getOutputPrinter() {
      return outputPrinter;
   }

   @Override
   public String toString() {
      return mediaTypeAsString;
   }
}
