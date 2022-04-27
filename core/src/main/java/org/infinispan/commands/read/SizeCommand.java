package org.infinispan.commands.read;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.util.ByteString;

/**
 * Command to calculate the size of the cache
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @since 4.0
 */
public class SizeCommand extends BaseRpcCommand implements FlagAffectedCommand, TopologyAffectedCommand {
   public static final byte COMMAND_ID = 61;

   private int topologyId = -1;
   private long flags = EnumUtil.EMPTY_BIT_SET;
   private IntSet segments;

   public SizeCommand(
         ByteString cacheName,
         IntSet segments,
         long flags
   ) {
      super(cacheName);
      setFlagsBitSet(flags);
      this.segments = segments;
   }

   public SizeCommand(ByteString name) {
      super(name);
   }

   // Only here for CommandIdUniquenessTest
   private SizeCommand() { super(null); }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitSizeCommand(ctx, this);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      InvocationContextFactory icf = registry.getInvocationContextFactory().running();
      InvocationContext ctx = icf.createRemoteInvocationContextForCommand(this, getOrigin());
      AsyncInterceptorChain invoker = registry.getInterceptorChain().running();
      return invoker.invokeAsync(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.DONT_LOAD;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   public long getFlagsBitSet() {
      return flags;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      this.flags = bitSet;
   }

   public IntSet getSegments() {
      return segments;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeLong(flags);
      output.writeObject(segments);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      setFlagsBitSet(input.readLong());
      segments = (IntSet) input.readObject();
   }

   @Override
   public String toString() {
      return "SizeCommand{" +
            "topologyId=" + topologyId +
            ", flags=" + flags +
            ", segments=" + segments +
            '}';
   }
}
