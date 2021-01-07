package org.infinispan.cli.completers;

import static org.infinispan.cli.completers.CacheCompleter.getCacheName;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class CacheConfigurationAttributeCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      throw new UnsupportedOperationException();
   }

   @Override
   protected Collection<String> getAvailableItems(ContextAwareCompleterInvocation invocation) throws IOException {
      Connection connection = invocation.context.getConnection();
      return getCacheName(invocation.context, invocation.getCommand()).map(connection::getCacheConfigurationAttributes).orElse(Collections.emptyList());
   }
}
