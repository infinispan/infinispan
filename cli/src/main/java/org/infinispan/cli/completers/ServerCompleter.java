package org.infinispan.cli.completers;

import java.io.IOException;
import java.util.Collection;

import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      return context.getConnection().getAvailableServers();
   }
}
