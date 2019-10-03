package org.infinispan.cli.completers;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.resources.CacheResource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class SiteCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      Connection connection = context.getConnection();
      CacheResource cache = connection.getActiveResource().findAncestor(CacheResource.class);
      return cache != null ? context.getConnection().getAvailableSites(connection.getActiveContainer().getName(), cache.getName()) : Collections.emptyList();
   }
}
