package org.infinispan.commands.read;

import java.util.Collection;

import org.infinispan.commands.AbstractTopologyAffectedCommand;
import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;

/**
 * Retrieves multiple entries at once.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class GetAllCommand extends AbstractTopologyAffectedCommand implements LocalCommand {
   public static final byte COMMAND_ID = 44;

   private Collection<?> keys;
   private boolean returnEntries;

   public GetAllCommand(Collection<?> keys, long flagsBitSet, boolean returnEntries) {
      super(flagsBitSet, -1);
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
   public byte getCommandId() {
      return COMMAND_ID;
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
   public String toString() {
      return "GetAllCommand{" +
            "keys=" + keys +
            ", returnEntries=" + returnEntries +
            ", flags=" + printFlags() +
            '}';
   }
}
