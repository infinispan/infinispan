package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
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
public class ClusteredGetCommand extends BaseClusteredReadCommand implements SegmentSpecificCommand {

   public static final byte COMMAND_ID = 16;
   private static final Log log = LogFactory.getLog(ClusteredGetCommand.class);
   private static final boolean trace = log.isTraceEnabled();

   private Object key;

   private InvocationContextFactory icf;
   private CommandsFactory commandsFactory;
   private AsyncInterceptorChain invoker;

   private InternalEntryFactory entryFactory;
   //only used by extended statistics. this boolean is local.
   private boolean isWrite;
   private int segment;

   private ClusteredGetCommand() {
      super(null, EnumUtil.EMPTY_BIT_SET); // For command id uniqueness test
   }

   public ClusteredGetCommand(ByteString cacheName) {
      super(cacheName, EnumUtil.EMPTY_BIT_SET);
   }

   public ClusteredGetCommand(Object key, ByteString cacheName, int segment, long flags) {
      super(cacheName, flags);
      this.key = key;
      this.isWrite = false;
      if (segment < 0) {
         throw new IllegalArgumentException("Segment must 0 or greater!");
      }
      this.segment = segment;
   }

   public void initialize(InvocationContextFactory icf, CommandsFactory commandsFactory,
                          InternalEntryFactory entryFactory, AsyncInterceptorChain interceptorChain) {
      this.icf = icf;
      this.commandsFactory = commandsFactory;
      this.invoker = interceptorChain;
      this.entryFactory = entryFactory;
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
      GetCacheEntryCommand command = commandsFactory.buildGetCacheEntryCommand(key, segment,
            EnumUtil.mergeBitSets(flagBitSet, getFlagsBitSet()));
      command.setTopologyId(topologyId);
      InvocationContext invocationContext = icf.createRemoteInvocationContextForCommand(command, getOrigin());
      CompletableFuture<Object> future = invoker.invokeAsync(invocationContext, command);
      return future.thenApply(rv -> {
         if (trace) log.tracef("Return value for key=%s is %s", key, rv);
         //this might happen if the value was fetched from a cache loader
         if (rv instanceof MVCCEntry) {
            MVCCEntry mvccEntry = (MVCCEntry) rv;
            return entryFactory.createValue(mvccEntry);
         } else if (rv instanceof InternalCacheEntry) {
            InternalCacheEntry internalCacheEntry = (InternalCacheEntry) rv;
            return internalCacheEntry.toInternalCacheValue();
         } else { // null or Response
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
   public void writeTo(ObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      MarshalledEntryUtil.writeKey(key, entryFactory, output);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = MarshalledEntryUtil.readKey(input);
      segment = UnsignedNumeric.readUnsignedInt(input);
      setFlagsBitSet(input.readLong());
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClusteredGetCommand that = (ClusteredGetCommand) o;

      return Objects.equals(key, that.key);
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(key);
   }

   @Override
   public String toString() {
      return new StringBuilder()
         .append("ClusteredGetCommand{key=")
         .append(key)
         .append(", flags=").append(printFlags())
         .append(", topologyId=").append(topologyId)
         .append("}")
         .toString();
   }

   public boolean isWrite() {
      return isWrite;
   }

   public void setWrite(boolean write) {
      isWrite = write;
   }

   @Override
   public int getSegment() {
      return segment;
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

}
