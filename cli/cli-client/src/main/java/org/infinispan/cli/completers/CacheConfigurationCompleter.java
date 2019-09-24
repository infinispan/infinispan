package org.infinispan.cli.completers;

import java.util.Collection;

import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.resources.ContainerResource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CacheConfigurationCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) {
      Connection connection = context.getConnection();
      return connection.getAvailableCacheConfigurations(connection.getActiveResource().findAncestor(ContainerResource.class).getName());
   }
}
