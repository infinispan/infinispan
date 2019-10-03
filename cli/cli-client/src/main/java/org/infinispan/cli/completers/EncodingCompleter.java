package org.infinispan.cli.completers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.cli.Context;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class EncodingCompleter extends ListCompleter {
   private static final List<String> ENCODINGS = Arrays.asList(
         MediaType.TEXT_PLAIN_TYPE,
         MediaType.APPLICATION_JSON_TYPE,
         MediaType.APPLICATION_XML_TYPE,
         MediaType.APPLICATION_OCTET_STREAM_TYPE
   );

   @Override
   Collection<String> getAvailableItems(Context context) {
      return ENCODINGS;
   }
}
