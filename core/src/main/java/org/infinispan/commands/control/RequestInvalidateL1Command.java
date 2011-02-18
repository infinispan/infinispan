package org.infinispan.commands.control;

import java.util.Arrays;
import java.util.Collection;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.L1Manager;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Requests that other data owners perform L1 invalidation for the given keys (used with DIST mode)
 *
 * @author Pete Muir
 * @since 5.0
 */
public class RequestInvalidateL1Command implements ReplicableCommand, VisitableCommand {
   public static final int COMMAND_ID = 18;
   
   private final Log log = LogFactory.getLog(RequestInvalidateL1Command.class);
   
   private L1Manager l1m;
   private Object[] keys;

   public RequestInvalidateL1Command() {
   }

   public RequestInvalidateL1Command(L1Manager l1m, Object... keys) {
      this.keys = keys;
      this.l1m = l1m;
   }

   public RequestInvalidateL1Command(L1Manager l1m, Collection<Object> keys) {
   	if (keys == null || keys.isEmpty())
         this.keys = new Object[]{};
      else
         this.keys = keys.toArray(new Object[keys.size()]);
      this.l1m = l1m;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   public void init(L1Manager l1m) {
      this.l1m = l1m;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
   	if (log.isTraceEnabled()) log.trace("Received request to invalidate keys for %s", Arrays.asList(keys));
   	// TODO what do to with the future here?
      l1m.flushLocalCache(Arrays.asList(keys));
      return null;
   }
   
   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor)
         throws Throwable {
      return visitor.visitRequestInvalidateL1Command(ctx, this);
   }
   
   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }
   
   public void setKeys(Object [] keys){
	   this.keys = keys;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      RequestInvalidateL1Command that = (RequestInvalidateL1Command) o;

      if (!Arrays.equals(keys, that.keys)) {
         return false;
      }

      return true;
   }

   @Override
   public Object[] getParameters() {
      if (keys == null || keys.length == 0) {
         return new Object[]{0};
      } else if (keys.length == 1) {
         return new Object[]{1, keys[0]};
      } else {
         Object[] retval = new Object[keys.length + 1];
         retval[0] = keys.length;
         System.arraycopy(keys, 0, retval, 1, keys.length);
         return retval;
      }
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      int size = (Integer) args[0];
      keys = new Object[size];
      if (size == 1) {
         keys[0] = args[1];
      } else if (size > 0) {
         System.arraycopy(args, 1, keys, 0, size);
      }
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (keys != null ? Arrays.hashCode(keys) : 0);
      return result;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "keys=" + Arrays.toString(keys) +
            '}';
   }

}
