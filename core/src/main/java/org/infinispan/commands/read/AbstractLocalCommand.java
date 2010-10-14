package org.infinispan.commands.read;

import org.infinispan.commands.LocalCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;

/**
 * Abstract class
 *
 * @author Manik Surtani
 * @since 4.1
 */
public class AbstractLocalCommand implements LocalCommand {
   private static final Object[] EMPTY_ARRAY = new Object[0];
   
   public byte getCommandId() {
      return 0;  // no-op
   }

   public Object[] getParameters() {
      return EMPTY_ARRAY;  // no-op
   }

   public void setParameters(int commandId, Object[] parameters) {
      // no-op
   }

   public boolean shouldInvoke(InvocationContext ctx) {
      return false;
   }

   protected boolean noTxModifications(InvocationContext ctx) {
      return !ctx.isInTxScope() || !((TxInvocationContext)ctx).hasModifications();
   }   
}
