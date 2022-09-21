package org.infinispan.xsite.irac;

import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.infinispan.util.ExponentialBackOff;
import org.testng.AssertJUnit;

public class ControlledExponentialBackOff implements ExponentialBackOff {

   private String name;
   private final BlockingDeque<Event> backOffEvents;
   private volatile CompletableFuture<Void> backOff = new CompletableFuture<>();

   ControlledExponentialBackOff() {
      backOffEvents = new LinkedBlockingDeque<>();
   }

   ControlledExponentialBackOff(String name) {
      this();
      this.name = name;
   }

   @Override
   public void reset() {
      backOffEvents.add(Event.RESET);
   }

   @Override
   public CompletionStage<Void> asyncBackOff() {
      CompletionStage<Void> stage = backOff;
      // add to the event after getting the completable future
      backOffEvents.add(Event.BACK_OFF);
      return stage;
   }

   void release() {
      backOff.complete(null);
      this.backOff = new CompletableFuture<>();
   }

   void cleanupEvents() {
      backOffEvents.clear();
   }

   void eventually(String message, Event... expected) {
      List<Event> events = new ArrayList<>(Arrays.asList(expected));
      while (!events.isEmpty()) {
         Event current = null;
         try {
            current = backOffEvents.poll(30, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            AssertJUnit.fail(e.getMessage());
         }
         assertTrue("At " + name + ": " + message + " Expected " + events + ", current " + current, events.contains(current));
         events.remove(current);
      }
   }

   void containsOnly(String message, Event event) {
      while (!backOffEvents.isEmpty()) {
         eventually(message, event);
      }
   }

   void assertNoEvents() {
      assertTrue("At " + name + ": Expected no events, found: " + backOffEvents, backOffEvents.isEmpty());
   }

   enum Event {
      BACK_OFF,
      RESET
   }

}
