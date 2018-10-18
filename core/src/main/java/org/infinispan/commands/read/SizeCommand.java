package org.infinispan.commands.read;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;

/**
 * Command to calculate the size of the cache
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
public class SizeCommand extends AbstractLocalCommand implements VisitableCommand {
   public SizeCommand(long flags) {
      setFlagsBitSet(flags);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitSizeCommand(ctx, this);
   }

   @Override
   public String toString() {
      return "SizeCommand{}";
   }
}
