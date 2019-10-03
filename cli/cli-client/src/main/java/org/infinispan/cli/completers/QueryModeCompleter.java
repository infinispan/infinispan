package org.infinispan.cli.completers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class QueryModeCompleter extends ListCompleter {
   private static final List<String> QUERY_MODES = Arrays.asList(
         "FETCH",
         "BROADCAST"
   );

   @Override
   Collection<String> getAvailableItems(Context context) {
      return QUERY_MODES;
   }
}
