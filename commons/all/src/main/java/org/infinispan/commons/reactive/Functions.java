package org.infinispan.commons.reactive;

import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.functions.Function;

public final class Functions {
   static final Function<Object, Object> IDENTITY = new Identity();
   public static final Action EMPTY_ACTION = new EmptyAction();
   static final Consumer<Object> EMPTY_CONSUMER = new EmptyConsumer();

   @SuppressWarnings("unchecked")
   public static <T> Function<T, T> identity() {
      return (Function<T, T>) IDENTITY;
   }

   @SuppressWarnings("unchecked")
   public static <T> Consumer<T> emptyConsumer() {
      return (Consumer<T>) EMPTY_CONSUMER;
   }

   static final class Identity implements Function<Object, Object> {
      public Object apply(Object v) {
         return v;
      }

      public String toString() {
         return "IdentityFunction";
      }
   }

   static final class EmptyAction implements Action {
      public void run() {
      }

      public String toString() {
         return "EmptyAction";
      }
   }

   static final class EmptyConsumer implements Consumer<Object> {
      public void accept(Object v) {
      }

      public String toString() {
         return "EmptyConsumer";
      }
   }
}
