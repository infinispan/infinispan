package org.infinispan.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.configuration.cache.TransactionConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
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

   private boolean isWrite;
   private Integer segment;

   private ClusteredGetCommand() {
      super(null, EnumUtil.EMPTY_BIT_SET); // For command id uniqueness test
   }

   public ClusteredGetCommand(ByteString cacheName) {
      super(cacheName, EnumUtil.EMPTY_BIT_SET);
   }

   public ClusteredGetCommand(Object key, ByteString cacheName, Integer segment, long flags) {
      super(cacheName, flags);
      this.key = key;
      this.isWrite = false;
      if (segment != null && segment < 0) {
         throw new IllegalArgumentException("Segment must 0 or greater!");
      }
      this.segment = segment;
   }

   /**
    * Invokes a logical "get(key)" on a remote cache and returns results.
    * @return
    */
   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry componentRegistry) throws Throwable {
      // make sure the get command doesn't perform a remote call
      // as our caller is already calling the ClusteredGetCommand on all the relevant nodes
      // CACHE_MODE_LOCAL is not used as it can be used when we want to ignore the ownership with respect to reads
      long flagBitSet = EnumUtil.bitSetOf(Flag.SKIP_REMOTE_LOOKUP);
      int segmentToUse;
      if (segment != null) {
         segmentToUse = segment;
      } else {
         segmentToUse = componentRegistry.getComponent(KeyPartitioner.class).getSegment(key);
      }
      // If this get command was due to a write and we are pessimistic that means it already holds the lock for the
      // given key - This allows expiration to be performed if needed as it won't have to acquire the lock
      // This code and the Flag can be removed when https://issues.redhat.com/browse/ISPN-12332 is complete
      if (isWrite) {
         TransactionConfiguration transactionConfiguration = componentRegistry.getConfiguration().transaction();
         if (transactionConfiguration.transactionMode() == TransactionMode.TRANSACTIONAL) {
            if (transactionConfiguration.lockingMode() == LockingMode.PESSIMISTIC) {
               flagBitSet = EnumUtil.mergeBitSets(flagBitSet, FlagBitSets.ALREADY_HAS_LOCK);
            }
         }
      }
      GetCacheEntryCommand command = componentRegistry.getCommandsFactory().buildGetCacheEntryCommand(key, segmentToUse,
            EnumUtil.mergeBitSets(flagBitSet, getFlagsBitSet()));
      command.setTopologyId(topologyId);
      InvocationContextFactory icf = componentRegistry.getInvocationContextFactory().running();
      InvocationContext invocationContext = icf.createRemoteInvocationContextForCommand(command, getOrigin());
      AsyncInterceptorChain invoker = componentRegistry.getInterceptorChain().running();
      return invoker.invokeAsync(invocationContext, command)
            .thenApply(rv -> {
               if (trace) log.tracef("Return value for key=%s is %s", key, rv);
               //this might happen if the value was fetched from a cache loader
               if (rv instanceof MVCCEntry) {
                  MVCCEntry<?, ?> mvccEntry = (MVCCEntry<?, ?>) rv;
                  InternalCacheValue<?> icv = componentRegistry.getInternalEntryFactory().wired().createValue(mvccEntry);
                  icv.setInternalMetadata(mvccEntry.getInternalMetadata());
                  return icv;
               } else if (rv instanceof InternalCacheEntry) {
                  InternalCacheEntry<?, ?> internalCacheEntry = (InternalCacheEntry<? ,?>) rv;
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
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      if (segment != null) {
         output.writeBoolean(true);
         UnsignedNumeric.writeUnsignedInt(output, segment);
      } else {
         output.writeBoolean(false);
      }
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(getFlagsBitSet()));
      output.writeBoolean(isWrite);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      boolean hasSegment = input.readBoolean();
      if (hasSegment) {
         segment = UnsignedNumeric.readUnsignedInt(input);
      }
      setFlagsBitSet(input.readLong());
      isWrite = input.readBoolean();
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
         .append(", isWrite=").append(isWrite)
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
}
