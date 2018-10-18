package org.infinispan.commands.read;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;

/**
 * Command implementation for {@link java.util.Map#entrySet()} functionality.
 *
 * @author Galder Zamarreño
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author William Burns
 * @since 4.0
 */
public class EntrySetCommand<K, V> extends AbstractLocalCommand implements VisitableCommand {
   public EntrySetCommand(long flagsBitSet) {
      setFlagsBitSet(flagsBitSet);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitEntrySetCommand(ctx, this);
   }

   @Override
   public String toString() {
      return "EntrySetCommand{" +
            ", flags=" + printFlags() +
            '}';
   }
}
