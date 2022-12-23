package org.infinispan.remoting.inboundhandler;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.transport.Address;

abstract class BaseControllingHandler {

   private final Address address;
   private final List<BlockHandlerImpl<ReplicableCommand>> replicableCmdBlockTest = new CopyOnWriteArrayList<>();
   private final ConcurrentHashMap<Class<?>, CountHandlerImpl> countingCommands = new ConcurrentHashMap<>(4);

   BaseControllingHandler(Address address) {
      this.address = address;
   }

   public CountHandler countRpc(Class<?> commandToCount) {
      return countingCommands.computeIfAbsent(commandToCount, aClass -> new CountHandlerImpl());
   }

   public <T extends ReplicableCommand> BlockHandler blockRpcBefore(Predicate<T> commandToBlock) {
      BlockHandlerImpl<ReplicableCommand> handler = new BlockHandlerImpl<>((Predicate<ReplicableCommand>) commandToBlock);
      replicableCmdBlockTest.add(handler);
      return handler;
   }

   public <T extends ReplicableCommand> BlockHandler blockRpcBefore(Class<T> commandToBlock) {
      return blockRpcBefore(commandToBlock::isInstance);
   }

   public void stopBlocking() {
      replicableCmdBlockTest.forEach(BlockHandlerImpl::unblock);
      replicableCmdBlockTest.clear();
   }

   void countCommand(ReplicableCommand command) {
      CountHandlerImpl handler = countingCommands.get(command.getClass());
      if (handler != null) {
         handler.increment();
      }
   }

   <T extends ReplicableCommand> void blockIfNeeded(T command, Runnable afterUnblocked) {
      for (BlockHandlerImpl<ReplicableCommand> handler : replicableCmdBlockTest) {
         if (handler.test(command)) {
            handler.runAfterBlocked(afterUnblocked);
            return;
         }
      }
      afterUnblocked.run();
   }

   @Override
   public String toString() {
      return getClass() + "{" +
            "address=" + address +
            '}';
   }
}
