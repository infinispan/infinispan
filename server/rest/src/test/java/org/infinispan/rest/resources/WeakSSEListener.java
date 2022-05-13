package org.infinispan.rest.resources;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.infinispan.commons.util.ByRef;
import org.infinispan.test.TestException;
import org.infinispan.util.KeyValuePair;

/**
 * A listener for SSE without ordering restriction.
 */
public class WeakSSEListener extends SSEListener {
   private final ConcurrentMap<String, List<String>> backup = new ConcurrentHashMap<>();

   @Override
   public void expectEvent(String type, String subString, Consumer<KeyValuePair<String, String>> consumer) throws InterruptedException {
      CompletableFuture<KeyValuePair<String, String>> waitEvent = CompletableFuture.supplyAsync(() -> {
         ByRef<KeyValuePair<String, String>> pair = new ByRef<>(null);
         backup.computeIfPresent(type, (k, v) -> {
            int index = -1;
            for (int i = 0; i < v.size() && pair.get() == null; i++) {
               if (v.get(i).contains(subString)) {
                  pair.set(new KeyValuePair<>(k, v.get(i)));
                  index = i;
                  break;
               }
            }

            if (index >= 0) v.remove(index);
            return v;
         });

         while (pair.get() == null) {
            try {
               KeyValuePair<String, String> event = events.poll(10, TimeUnit.SECONDS);
               assert event != null : "No event received";

               if (type.equals(event.getKey()) && event.getValue().contains(subString)) {
                  pair.set(event);
                  break;
               } else {
                  backup.compute(event.getKey(), (k, v) -> {
                     if (v == null) v = new ArrayList<>();

                     v.add(event.getValue());
                     return v;
                  });
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new TestException(e);
            }
         }

         assert pair.get() != null : "Should contain event with: " + subString;
         return pair.get();
      });

      try {
         KeyValuePair<String, String> pair = waitEvent.get(10, TimeUnit.SECONDS);
         consumer.accept(pair);
      } catch (ExecutionException | TimeoutException e) {
         throw new TestException(e);
      }
   }
}
