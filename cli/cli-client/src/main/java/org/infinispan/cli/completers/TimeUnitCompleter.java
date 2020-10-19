package org.infinispan.cli.completers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class TimeUnitCompleter extends ListCompleter {
   private static List<String> VALUES = Arrays.stream(TimeUnit.values()).map(TimeUnit::name).collect(Collectors.toList());

   @Override
   Collection<String> getAvailableItems(Context context) {
      return VALUES;
   }
}
