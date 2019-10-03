package org.infinispan.cli.completers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CounterStorageCompleter extends ListCompleter {
   private static final List<String> COUNTER_STORAGE_TYPES = Arrays.asList(
         "VOLATILE", "PERSISTENT"
   );

   @Override
   Collection<String> getAvailableItems(Context context) {
      return COUNTER_STORAGE_TYPES;
   }
}
