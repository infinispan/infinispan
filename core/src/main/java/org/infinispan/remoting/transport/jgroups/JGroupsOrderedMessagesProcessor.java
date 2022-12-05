package org.infinispan.remoting.transport.jgroups;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.jgroups.Address;
import org.jgroups.Message;

/**
 * TODO! document this
 */
public class JGroupsOrderedMessagesProcessor {

   private final Executor executor;
   private final Consumer<Message> processor;
   private final ConcurrentHashMap<Address, MessageOrderedQueue> orderedQueues = new ConcurrentHashMap<>(16);

   public JGroupsOrderedMessagesProcessor(Executor executor, Consumer<Message> processor) {
      this.executor = executor;
      this.processor = processor;
   }

   public void processSingle(Message message) {
      orderedQueues.compute(message.src(), (address, queue) -> {
         if (queue == null) {
            queue = new MessageOrderedQueue(address);
         }
         queue.processSingle(message);
         return queue;
      });
   }

   public void processMultiple(Address src, List<Message> messages) {
      orderedQueues.compute(src, (address, queue) -> {
         if (queue == null) {
            queue = new MessageOrderedQueue(src);
         }
         queue.processMultiples(messages);
         return queue;
      });
   }



   private final class MessageOrderedQueue implements Runnable {
      private final Address sender;
      private CompletableFuture<Void> lastMessage;

      private MessageOrderedQueue(Address sender) {
         this.sender = sender;
      }

      void processSingle(Message message) {
         if (lastMessage == null) {
            lastMessage = handle(message);
            return;
         }
         lastMessage = lastMessage.thenCompose(unused -> handle(message));
      }

      void processMultiples(List<Message> messages) {
         if (lastMessage == null) {
            lastMessage = handle(messages);
            return;
         }
         lastMessage = lastMessage.thenCompose(unused -> handle(messages));
      }

      private CompletableFuture<Void> handle(Message message) {
         return CompletableFuture.runAsync(() -> processor.accept(message), executor)
               .exceptionally(CompletableFutures.toNullFunction())
               .thenRun(this);
      }

      private CompletableFuture<Void> handle(List<Message> messages) {
         return CompletableFuture.runAsync(() -> messages.forEach(processor), executor)
               .exceptionally(CompletableFutures.toNullFunction())
               .thenRun(this);
      }

      @Override
      public void run() {
         // remove if done
         orderedQueues.compute(sender, (address, queue) -> {
            if (queue == null || queue.lastMessage == null || queue.lastMessage.isDone()) {
               return null;
            }
            return queue;
         });
      }
   }

}
