package org.infinispan.cli.completers;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.resources.ContainerResource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CounterCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      Connection connection = context.getConnection();
      ContainerResource container = connection.getActiveResource().findAncestor(ContainerResource.class);
      return container != null ? connection.getAvailableCounters(container.getName()) : Collections.emptyList();
   }
}
