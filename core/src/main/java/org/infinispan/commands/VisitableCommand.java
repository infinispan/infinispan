package org.infinispan.commands;

import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.lifecycle.ComponentStatus;


/**
 * A type of command that can accept {@link Visitor}s, such as {@link org.infinispan.interceptors.base.CommandInterceptor}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public interface VisitableCommand extends ReplicableCommand {
   /**
    * Accept a visitor, and return the result of accepting this visitor.
    *
    * @param ctx     invocation context
    * @param visitor visitor to accept
    * @return arbitrary return value
    * @throws Throwable in the event of problems
    */
   Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable;

   /**
    * Used by the InboundInvocationHandler to determine whether the command should be invoked or not.
    * @return true if the command should be invoked, false otherwise.
    */
   boolean shouldInvoke(InvocationContext ctx);

   /**
    * Similar to {@link #shouldInvoke(InvocationContext)} but evaluated by {@link InvocationContextInterceptor}.
    * Commands can opt to be discarded in case the cache status is not suited (as {@link InvalidateCommand})
    * @return true if the command should NOT be invoked.
    */
   boolean ignoreCommandOnStatus(ComponentStatus status);

   /**
    * @return {@code true} if the command needs to read the previous values of the keys it acts on.
    */
   boolean readsExistingValues();

   /**
    * @return {@code true} if the command needs to read the previous values even on the backup owners.
    *   In transactional caches, this refers to all the owners except the originator.
    */
   default boolean alwaysReadsExistingValues() {
      return false;
   }
}
