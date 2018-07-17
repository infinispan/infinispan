package org.infinispan.topology;

import java.io.ObjectInput;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.marshall.core.UserObjectOutput;

/**
 * A hear-beat command used to ping members in {@link ClusterTopologyManagerImpl#confirmMembersAvailable()}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class HeartBeatCommand implements ReplicableCommand {

   public static final byte COMMAND_ID = 30;
   public static final HeartBeatCommand INSTANCE = new HeartBeatCommand();


   @Override
   public byte getCommandId() {
      return COMMAND_ID;
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
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory){
      //nothing to write
   }

   @Override
   public void readFrom(ObjectInput input) {
      //nothing to read
   }
}
