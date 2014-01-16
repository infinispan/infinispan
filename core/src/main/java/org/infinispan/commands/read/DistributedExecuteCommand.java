package org.infinispan.commands.read;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.infinispan.Cache;
import org.infinispan.commands.CancellableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.context.InvocationContext;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distexec.spi.DistributedTaskLifecycleService;
import org.infinispan.lifecycle.ComponentStatus;

/**
 * DistributedExecuteCommand is used to migrate Callable and execute it in remote JVM.
 *
 * @author Vladimir Blagojevic
 * @author Mircea Markus
 * @since 5.0
 */

public class DistributedExecuteCommand<V> extends BaseRpcCommand implements VisitableCommand, CancellableCommand{

   public static final int COMMAND_ID = 19;

   private static final long serialVersionUID = -7828117401763700385L;

   private Cache<Object, Object> cache;

   private Set<Object> keys;

   private Callable<V> callable;

   private UUID uuid;

   public DistributedExecuteCommand(String cacheName) {
      this(cacheName, null, null);
   }

   public DistributedExecuteCommand(String cacheName, Collection<Object> inputKeys, Callable<V> callable) {
      super(cacheName);
      if (inputKeys == null || inputKeys.isEmpty())
         this.keys = InfinispanCollections.emptySet();
      else
         this.keys = new HashSet<Object>(inputKeys);
      this.callable = callable;
      this.uuid = UUID.randomUUID();
   }

   public DistributedExecuteCommand() {
      this(null, null, null);
   }

   public void init(Cache<Object, Object> cache) {
      this.cache = cache;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitDistributedExecuteCommand(ctx, this);
   }

   @Override
   public boolean ignoreCommandOnStatus(ComponentStatus status) {
      return false;
   }

   @Override
   public boolean shouldInvoke(InvocationContext ctx) {
      return true;
   }

   /**
    * Performs invocation of Callable and returns result
    *
    * @param context
    *           invocation context
    * @return result of Callable invocations
    */
   @Override
   public V perform(InvocationContext context) throws Exception {
      // hook into lifecycle
      DistributedTaskLifecycleService taskLifecycleService = DistributedTaskLifecycleService.getInstance();
      Callable<V> callable = getCallable();
      V result = null;
      try {
         taskLifecycleService.onPreExecute(callable, cache);
         if (callable instanceof DistributedCallable<?, ?, ?>) {
            DistributedCallable<Object, Object, Object> dc = (DistributedCallable<Object, Object, Object>) callable;
            dc.setEnvironment(cache, keys);
         }
         result = callable.call();
      } finally {
         taskLifecycleService.onPostExecute(callable);
      }
      return result;
   }

   public Callable<V> getCallable() {
      return callable;
   }

   public Set<Object> getKeys(){
      return keys;
   }

   public boolean hasKeys(){
      return keys.isEmpty();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public UUID getUUID() {
      return uuid;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] { keys, callable, uuid};
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id");
      int i = 0;
      this.keys = (Set<Object>) args[i++];
      this.callable = (Callable<V>) args[i++];
      this.uuid = (UUID) args[i++];
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((keys == null) ? 0 : keys.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof DistributedExecuteCommand)) {
         return false;
      }
      DistributedExecuteCommand other = (DistributedExecuteCommand) obj;
      if (keys == null) {
         if (other.keys != null) {
            return false;
         }
      } else if (!keys.equals(other.keys)) {
         return false;
      }
      return true;
   }

   @Override
   public String toString() {
      return "DistributedExecuteCommand [cache=" + cache + ", keys=" + keys + ", callable="
               + callable + "]";
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

}
