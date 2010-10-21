package org.infinispan.commands.read;

import org.infinispan.commands.LocalCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;

import java.util.HashSet;
import java.util.Set;

/**
 * Abstract class
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
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

   protected Set<Object> getKeySetWithinTransaction(InvocationContext ctx, DataContainer container) {
      Set<Object> objects = container.keySet();
      Set<Object> result = new HashSet<Object>();
      result.addAll(objects);
      for (CacheEntry ce : ctx.getLookedUpEntries().values()) {
         if (ce.isRemoved()) {
            result.remove(ce.getKey());
         } else {
            if (ce.isCreated()) {
               result.add(ce.getKey());
            }
         }
      }
      return result;
   }
}
