package org.infinispan.server.cli.handlers;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.server.cli.util.InfinispanUtil;
import org.jboss.as.cli.CommandContext;
import org.jboss.as.cli.CommandLineCompleter;
import org.jboss.as.cli.impl.DefaultCompleter;

/**
 * The {@link CommandLineCompleter} implementation that shows all the caches name
 * under the current cache container.
 *
 * @author Pedro Ruivo
 * @since 6.1
 */
public class CacheNameCommandCompleter implements CommandLineCompleter {

   private final DefaultCompleter completer;

   public CacheNameCommandCompleter() {
      completer = new DefaultCompleter(new DefaultCompleter.CandidatesProvider() {
         @Override
         public Collection<String> getAllCandidates(CommandContext ctx) {
            try {
               Map<String, List<String>> caches = InfinispanUtil.getCachesNames(ctx, InfinispanUtil.getCacheInfo(ctx)
                     .getContainer());
               Set<String> cachesName = new HashSet<String>();
               for (List<String> cacheName : caches.values()) {
                  cachesName.addAll(cacheName);
               }
               return cachesName;
            } catch (Exception e) {
               return Collections.emptyList();
            }
         }
      });
   }

   @Override
   public int complete(CommandContext ctx, String buffer, int cursor, List<String> candidates) {
      return completer.complete(ctx, buffer, cursor, candidates);
   }
}
