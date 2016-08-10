package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Issues a remote get call.  This is not a {@link org.infinispan.commands.VisitableCommand} and hence not passed up the
 * interceptor chain.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ClusteredGetCommand extends BaseClusteredReadCommand implements TopologyAffectedCommand {

   public static final byte COMMAND_ID = 16;
   private static final Log log = LogFactory.getLog(ClusteredGetCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private Object key;
   private int topologyId = -1;

   private InvocationContextFactory icf;
   private CommandsFactory commandsFactory;
   private AsyncInterceptorChain invoker;

   private InternalEntryFactory entryFactory;
   private Equivalence keyEquivalence;
   //only used by extended statistics. this boolean is local.
   private boolean isWrite;

   private ClusteredGetCommand() {
      super(null, EnumUtil.EMPTY_BIT_SET); // For command id uniqueness test
   }

   public ClusteredGetCommand(ByteString cacheName) {
      super(cacheName, EnumUtil.EMPTY_BIT_SET);
   }

   public ClusteredGetCommand(Object key, ByteString cacheName, long flags, Equivalence keyEquivalence) {
      super(cacheName, flags);
      this.key = key;
      this.keyEquivalence = keyEquivalence;
      this.isWrite = false;
   }

   public void initialize(InvocationContextFactory icf, CommandsFactory commandsFactory, InternalEntryFactory entryFactory,
                          AsyncInterceptorChain interceptorChain, Equivalence keyEquivalence) {
      this.icf = icf;
      this.commandsFactory = commandsFactory;
      this.invoker = interceptorChain;
      this.entryFactory = entryFactory;
      this.keyEquivalence = keyEquivalence;
   }

   /**
    * Invokes a logical "get(key)" on a remote cache and returns results.
    */
   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      // make sure the get command doesn't perform a remote call
      // as our caller is already calling the ClusteredGetCommand on all the relevant nodes
      // CACHE_MODE_LOCAL is not used as it can be used when we want to ignore the ownership with respect to reads
      long flagBitSet = EnumUtil.bitSetOf(Flag.SKIP_REMOTE_LOOKUP);
      GetCacheEntryCommand command = commandsFactory.buildGetCacheEntryCommand(key, EnumUtil.mergeBitSets(flagBitSet, getFlagsBitSet()));
      command.setTopologyId(topologyId);
      InvocationContext invocationContext = icf.createRemoteInvocationContextForCommand(command, getOrigin());
      CompletableFuture<Object> future = invoker.invokeAsync(invocationContext, command);
      return future.thenApply(rv -> {
         //this might happen if the value was fetched from a cache loader
         if (rv instanceof MVCCEntry) {
            if (trace) log.trace("Handling an internal cache entry...");
            MVCCEntry mvccEntry = (MVCCEntry) rv;
            return entryFactory.createValue(mvccEntry);
         } else if (rv instanceof InternalCacheEntry) {
            InternalCacheEntry internalCacheEntry = (InternalCacheEntry) rv;
            return internalCacheEntry.toInternalCacheValue();
         } else {
            return rv;
         }
      });
   }

   @Deprecated
   public GlobalTransaction getGlobalTransaction() {
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeLong(Flag.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      setFlagsBitSet(input.readLong());
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClusteredGetCommand that = (ClusteredGetCommand) o;

      return !(key != null ?
         !(keyEquivalence != null ? keyEquivalence.equals(key, that.key) : key.equals(that.key))
         : that.key != null);
   }

   @Override
   public int hashCode() {
      int result;
      result = (key != null
          ? (keyEquivalence != null ? keyEquivalence.hashCode(key) : key.hashCode())
          : 0);
      return result;
   }

   @Override
   public String toString() {
      return new StringBuilder()
         .append("ClusteredGetCommand{key=")
         .append(key)
         .append(", flags=").append(printFlags())
         .append("}")
         .toString();
   }

   public boolean isWrite() {
      return isWrite;
   }

   public void setWrite(boolean write) {
      isWrite = write;
   }

   public Object getKey() {
      return key;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return false;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }
}
