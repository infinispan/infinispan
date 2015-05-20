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

   Cache<Object, ?> getFlagRespectingCache() {
      Set<Flag> flags = getFlags();
      if (flags != null  && !flags.isEmpty()) {
         return cache.getAdvancedCache().withFlags(flags.toArray(new Flag[flags.size()]));
      } else {
         return cache;
      }
   }

   @Override
   public Integer perform(InvocationContext ctx) throws Throwable {
      long size = getFlagRespectingCache().keySet().stream().count();
      if (size > Integer.MAX_VALUE) {
         return Integer.MAX_VALUE;
      } else {
         return (int) size;
      }
   }

   @Override
   public String toString() {
      return "SizeCommand{}";
   }
}
