package org.infinispan.cli.completers;

import java.util.Collection;

import org.infinispan.cli.Context;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CacheConfigurationCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) {
      return context.getConnection().getAvailableCacheConfigurations();
   }
}
