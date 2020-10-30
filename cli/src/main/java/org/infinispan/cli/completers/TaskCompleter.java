package org.infinispan.cli.completers;

import java.io.IOException;
import java.util.Collection;

import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
public class TaskCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      Connection connection = context.getConnection();
      return connection.getAvailableTasks(connection.getActiveContainer().getName());
   }
}
