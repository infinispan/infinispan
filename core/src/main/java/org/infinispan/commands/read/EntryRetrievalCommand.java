package org.infinispan.commands.read;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.iteration.EntryIterable;
import org.infinispan.iteration.impl.EntryIterableImpl;
import org.infinispan.iteration.impl.EntryRetriever;

import java.util.EnumSet;
import java.util.Set;

/**
 * Command that returns an iterable instance that can be used to produce iterators that work over the entire
 * cache.
 *
 * @author wburns
 * @since 7.0
 */
public class EntryRetrievalCommand<K, V> extends AbstractLocalCommand implements VisitableCommand {
   private final KeyValueFilter<K, V> filter;
   private final EntryRetriever<K, V> retriever;
   private final Cache<K, V> cache;

   public EntryRetrievalCommand(KeyValueFilter<K, V> filter, EntryRetriever<K, V> retriever, Set<Flag> flags,
                                Cache<K, V> cache) {
      setFlags(flags);
      this.filter = filter;
      this.retriever = retriever;
      this.cache = cache;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitEntryRetrievalCommand(ctx, this);
   }

   @Override
   public EntryIterable<K, V> perform(InvocationContext ctx) throws Throwable {
      // We need to copy the flag set since it is possible to modify the flags after retrieving the
      // EntryIterable and we don't want it to effect that.
      return new EntryIterableImpl<>(retriever, filter, flags != null ? EnumSet.copyOf(flags) :
            EnumSet.noneOf(Flag.class), cache);
   }
}
