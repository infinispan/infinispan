package org.infinispan.rest.operations.mediatypes;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.rest.operations.mediatypes.impl.BinaryOutputPrinter;
import org.infinispan.rest.operations.mediatypes.impl.TextOutputPrinter;
import org.infinispan.rest.operations.mediatypes.impl.HTMLOutputPrinter;
import org.infinispan.rest.operations.mediatypes.impl.JSONOutputPrinter;
import org.infinispan.rest.operations.mediatypes.impl.XMLOutputPrinter;

/**
 * Represents Media Type with attached {@link OutputPrinter}.
 *
 * @author Sebastian Łaskawiec
 */
public enum MediaType {

   /**
    * Represents <code>text/plain</code>.
    */
   TEXT_PLAIN("text/plain", new TextOutputPrinter(), true),

   /**
    * Represents <code>text/html</code>.
    */
   TEXT_HTML("text/html", new HTMLOutputPrinter(), true),

   /**
    * Represents <code>application/text</code>. Similar to TEXT_PLAIN.
    */
   APPLICATION_TEXT("application/text", new TextOutputPrinter(), true),

   /**
    * Represents <code>application/xml</code>.
    */
   APPLICATION_XML("application/xml", new XMLOutputPrinter(), true),

   /**
    * Represents <code>application/json</code>.
    */
   APPLICATION_JSON("application/json", new JSONOutputPrinter(), true),

   /**
    * Represents <code>application/octet-stream</code>.
    */
   APPLICATION_OCTET_STREAM("application/octet-stream", new BinaryOutputPrinter(), false),

   /**
    * Represents <code>image/png</code>.
    */
   IMAGE_PNG("image/png", new BinaryOutputPrinter(), false),

   /**
    * Represents <code>application/binary</code>. Similar to APPLICATION_OCTET_STREAM.
    */
   APPLICATION_BINARY("application/binary", new BinaryOutputPrinter(), false),

   /**
    * Represents <code>application/x-java-serialized-object</code>. Similar to APPLICATION_OCTET_STREAM.
    */
   APPLICATION_SERIALIZED_OBJECT("application/x-java-serialized-object", new BinaryOutputPrinter(), false);

   private static final Map<String, MediaType> reverseLookup = new HashMap<>(MediaType.values().length);

   static {
      for(MediaType type : MediaType.values()) {
         reverseLookup.put(type.mediaTypeAsString, type);
      }
   }

   private final String mediaTypeAsString;
   private final OutputPrinter outputPrinter;
   private final boolean needsCharset;

   MediaType(String mediaTypeAsString, OutputPrinter outputPrinter, boolean needsCharset) {
      this.mediaTypeAsString = mediaTypeAsString;
      this.outputPrinter = outputPrinter;
      this.needsCharset = needsCharset;
   }

   /**
    * Converts text into {@link MediaType}.
    *
    * @param text text to be parsed.
    * @return MediaType value or <code>null</code> if no mapping is found.
    */
   public static MediaType fromMediaTypeAsString(String text) {
      String[] suppliedMediaTypes = text.split(" *, *");
      for (int i = 0; i < suppliedMediaTypes.length; ++i) {
         MediaType decodedMediaType = reverseLookup.get(sanitize(suppliedMediaTypes[i]));
         if (decodedMediaType != null) {
            return decodedMediaType;
         }
      }
      return null;
   }

   private static String sanitize(String text) {
      int charsetSeparator = text.indexOf(';');
      if (charsetSeparator != -1) {
         text = text.substring(0, text.indexOf(";"));
      }
      return text;
   }

   /**
    * Returns {@link OutputPrinter} associated to this {@link MediaType}.
    *
    * @return OutputPrinter instance.
    */
   public OutputPrinter getOutputPrinter() {
      return outputPrinter;
   }

   @Override
   public String toString() {
      return mediaTypeAsString;
   }

   public boolean needsCharset() {
      return this.needsCharset;
   }
}
