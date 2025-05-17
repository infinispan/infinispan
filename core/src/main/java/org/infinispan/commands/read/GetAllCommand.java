package org.infinispan.commands.read;

import java.util.Collection;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.util.ByteString;

/**
 * Retrieves multiple entries at once.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class GetAllCommand extends AbstractTopologyAffectedCommand implements LocalCommand {

   private Collection<?> keys;
   private boolean returnEntries;

   public GetAllCommand(ByteString cacheName, Collection<?> keys, long flagsBitSet, boolean returnEntries) {
      super(cacheName, flagsBitSet, -1);
      this.keys = keys;
      this.returnEntries = returnEntries;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitGetAllCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      return LoadType.PRIMARY;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   public boolean isReturnEntries() {
      return returnEntries;
   }

   public Collection<?> getKeys() {
      return keys;
   }

   public void setKeys(Collection<?> keys) {
      this.keys = keys;
   }

   @Override
   public NodeVersion supportedSince() {
      return NodeVersion.SIXTEEN;
   }

   @Override
   public String toString() {
      return "GetAllCommand{" +
            "keys=" + keys +
            ", returnEntries=" + returnEntries +
            ", flags=" + printFlags() +
            '}';
   }
}
