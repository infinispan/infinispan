package org.infinispan.cli.completers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class LogLevelCompleter extends ListCompleter {
   private static final List<String> LEVELS = Arrays.asList(
         "OFF",
         "TRACE",
         "DEBUG",
         "INFO",
         "WARN",
         "ERROR",
         "FATAL",
         "ALL"
   );

   @Override
   Collection<String> getAvailableItems(Context context) {
      return LEVELS;
   }
}
