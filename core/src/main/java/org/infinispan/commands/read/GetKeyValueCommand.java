package org.infinispan.commands.read;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;

/**
 * Implements functionality defined by {@link org.infinispan.Cache#get(Object)} and
 * {@link org.infinispan.Cache#containsKey(Object)} operations
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class GetKeyValueCommand extends AbstractDataCommand implements LocalCommand {

   public GetKeyValueCommand(Object key, int segment, long flagsBitSet) {
      super(key, segment, flagsBitSet);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetKeyValueCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.OWNER;
   }

   public String toString() {
      return "GetKeyValueCommand {key=" +
            toStr(key) +
            ", flags=" + printFlags() +
            "}";
   }
}
