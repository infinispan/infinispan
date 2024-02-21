package org.infinispan.cli.completers;

import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CounterCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      Connection connection = context.getConnection();
      return connection != null ? connection.getAvailableCounters() : Collections.emptyList();
   }
}
