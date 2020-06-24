package org.infinispan.cli.completers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.cli.Context;
import org.openjdk.jmh.annotations.Mode;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public class BenchmarkModeCompleter extends ListCompleter {
   private static List<String> VALUES = Arrays.stream(Mode.values()).map(Mode::name).collect(Collectors.toList());

   @Override
   Collection<String> getAvailableItems(Context context) {
      return VALUES;
   }
}
