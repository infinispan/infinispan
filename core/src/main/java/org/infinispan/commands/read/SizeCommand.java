package org.infinispan.commands.read;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.NullValueConverter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Command to calculate the size of the cache
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
public class SizeCommand extends AbstractLocalCommand implements VisitableCommand {
   private final Cache<Object, ?> cache;

   public SizeCommand(Cache<Object, ?> cache, Set<Flag> flags) {
      setFlags(flags);
      this.cache = cache;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitSizeCommand(ctx, this);
   }

   @Override
   public Integer perform(InvocationContext ctx) throws Throwable {
      int size = 0;
      Map<Object, CacheEntry> contextEntries = ctx.getLookedUpEntries();
      // Keeps track of keys that were found in the context, which means to not count them later
      Set<Object> keys = new HashSet<>();
      try (CloseableIterable<CacheEntry<Object, Void>> iterator = cache.getAdvancedCache().withFlags(
            flags != null ? flags.toArray(new Flag[flags.size()]) : null).filterEntries(AcceptAllKeyValueFilter.getInstance()).converter(
            NullValueConverter.getInstance())) {
         for (CacheEntry<Object, Void> entry : iterator) {
            CacheEntry value = contextEntries.get(entry.getKey());
            if (value != null) {
               keys.add(entry.getKey());
               if (!value.isRemoved()) {
                  if (size++ == Integer.MAX_VALUE) {return Integer.MAX_VALUE;}
               }
            } else {
               if (size++ == Integer.MAX_VALUE) {return Integer.MAX_VALUE;}
            }
         }
      }

      // We can only add context entries if we didn't see it in iterator and it isn't removed
      for (Map.Entry<Object, CacheEntry> entry : contextEntries.entrySet()) {
         if (!keys.contains(entry.getKey()) && !entry.getValue().isRemoved()) {
            if (size++ == Integer.MAX_VALUE) { return Integer.MAX_VALUE; }
         }
      }

      return size;
   }

   @Override
   public String toString() {
      return "SizeCommand{}";
   }
}
