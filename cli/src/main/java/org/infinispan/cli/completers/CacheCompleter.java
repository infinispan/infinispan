package org.infinispan.cli.completers;

import org.aesh.command.Command;
import org.infinispan.cli.Context;
import org.infinispan.cli.commands.CacheAwareCommand;
import org.infinispan.cli.resources.CacheResource;
import org.infinispan.cli.resources.Resource;

import java.util.Collection;
import java.util.Optional;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CacheCompleter extends ListCompleter {
   @Override
   Collection<String> getAvailableItems(Context context) {
      return context.getConnection().getAvailableCaches();
   }

   static Optional<String> getCacheName(Context context, Command<?> command) {
      Resource resource = context.getConnection().getActiveResource();
      if (command instanceof CacheAwareCommand) {
         return ((CacheAwareCommand) command).getCacheName(resource);
      }
      return CacheResource.findCacheName(resource);
   }
}
