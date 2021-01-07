package org.infinispan.cli.completers;

import static org.infinispan.cli.completers.CacheCompleter.getCacheName;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.aesh.command.Command;
import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class SiteCompleter extends ListCompleter {

   @Override
   protected Collection<String> getAvailableItems(ContextAwareCompleterInvocation invocation) throws IOException {
      Context context = invocation.context;
      Command<?> cmd = invocation.getCommand();
      Connection connection = context.getConnection();
      Optional<String> cacheName = getCacheName(context, cmd);
      return cacheName.map(name -> getAvailableSites(connection, name))
            .orElseGet(connection::getSitesView);
   }

   @Override
   Collection<String> getAvailableItems(Context context) throws IOException {
      throw new IllegalStateException();
   }

   private static Collection<String> getAvailableSites(Connection connection, String cacheName) {
      try {
         return connection.getAvailableSites(connection.getActiveContainer().getName(), cacheName);
      } catch (Exception e) {
         return Collections.emptyList();
      }
   }
}
