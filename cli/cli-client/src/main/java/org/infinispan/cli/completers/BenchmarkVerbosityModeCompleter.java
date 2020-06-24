package org.infinispan.cli.completers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.cli.Context;
import org.openjdk.jmh.runner.options.VerboseMode;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class BenchmarkVerbosityModeCompleter extends ListCompleter {
   private static List<String> VALUES = Arrays.stream(VerboseMode.values()).map(VerboseMode::name).collect(Collectors.toList());

   @Override
   Collection<String> getAvailableItems(Context context) {
      return VALUES;
   }
}
