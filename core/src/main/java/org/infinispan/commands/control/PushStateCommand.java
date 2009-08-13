package org.infinispan.commands.control;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.remoting.transport.Address;

import java.util.Map;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = ReplicableCommandExternalizer.class, id = Ids.PUSH_STATE_COMMAND)
public class PushStateCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 20;
   Address sender;
   Map<Object, InternalCacheValue> state;

   public PushStateCommand() {
   }

   public PushStateCommand(String cacheName, Address sender, Map<Object, InternalCacheValue> state) {
      super(cacheName);
      this.sender = sender;
      this.state = state;
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      return null;  // TODO: Customise this generated block
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{cacheName, sender, state};
   }

   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] parameters) {
      cacheName = (String) parameters[0];
      sender = (Address) parameters[1];
      state = (Map<Object, InternalCacheValue>) parameters[2];
   }
}
