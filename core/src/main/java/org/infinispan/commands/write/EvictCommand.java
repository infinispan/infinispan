package org.infinispan.commands.write;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class EvictCommand extends RemoveCommand implements LocalCommand {
   public EvictCommand(Object key, int segment, long flagsBitSet, CommandInvocationId commandInvocationId) {
      super(key, null, false, segment, flagsBitSet, commandInvocationId);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitEvictCommand(ctx, this);
   }

   @Override
   public String toString() {
      return new StringBuilder()
         .append("EvictCommand{key=")
         .append(key)
         .append(", value=").append(value)
         .append(", flags=").append(printFlags())
         .append("}")
         .toString();
   }

   @Override
   public LoadType loadType() {
      return LoadType.DONT_LOAD;
   }
}
