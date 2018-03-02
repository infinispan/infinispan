package org.infinispan.util.rxjava;

import java.util.PrimitiveIterator;
import java.util.function.IntFunction;

import org.infinispan.commons.util.IntSet;
import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;
import io.reactivex.annotations.Nullable;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.internal.functions.ObjectHelper;
import io.reactivex.internal.fuseable.ConditionalSubscriber;
import io.reactivex.internal.subscriptions.BasicQueueSubscription;
import io.reactivex.internal.subscriptions.EmptySubscription;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;

/**
 * Flowable implementation that will produce entries by providing every int in the given set to the provided IntFunction.
 * @author wburns
 * @since 9.4
 */
// This class was copied from FlowableFromIterable.java in rxjava 2.1.3. It was tweaked to support
// IntSet/PrimitiveIterator and invoke IntFunction per entry
public class FlowableFromIntSetFunction<T> extends Flowable<T> {

   final IntSet intSet;
   final IntFunction<T> intFunction;

   public FlowableFromIntSetFunction(IntSet intSet, IntFunction<T> intFunction) {
      this.intSet = intSet;
      this.intFunction = intFunction;
   }

   @Override
   public void subscribeActual(Subscriber<? super T> s) {
      PrimitiveIterator.OfInt it;
      try {
         it = intSet.iterator();
      } catch (Throwable e) {
         Exceptions.throwIfFatal(e);
         EmptySubscription.error(e, s);
         return;
      }

      subscribe(s, it, intFunction);
   }

   public static <T> void subscribe(Subscriber<? super T> s, PrimitiveIterator.OfInt it, IntFunction<T> intFunction) {
      boolean hasNext;
      try {
         hasNext = it.hasNext();
      } catch (Throwable e) {
         Exceptions.throwIfFatal(e);
         EmptySubscription.error(e, s);
         return;
      }

      if (!hasNext) {
         EmptySubscription.complete(s);
         return;
      }

      if (s instanceof ConditionalSubscriber) {
         s.onSubscribe(new IteratorConditionalSubscription<T>((ConditionalSubscriber<? super T>)s, it, intFunction));
      } else {
         s.onSubscribe(new IteratorSubscription<T>(s, it, intFunction));
      }
   }

   abstract static class BaseRangeSubscription<T> extends BasicQueueSubscription<T> {
      private static final long serialVersionUID = -2252972430506210021L;

      PrimitiveIterator.OfInt it;
      final IntFunction<T> intFunction;

      volatile boolean cancelled;

      boolean once;

      BaseRangeSubscription(PrimitiveIterator.OfInt it, IntFunction<T> intFunction) {
         this.it = it;
         this.intFunction = intFunction;
      }

      @Override
      public final int requestFusion(int mode) {
         return mode & SYNC;
      }

      @Nullable
      @Override
      public final T poll() {
         if (it == null) {
            return null;
         }
         if (!once) {
            once = true;
         } else {
            if (!it.hasNext()) {
               return null;
            }
         }
         T val = intFunction.apply(it.nextInt());
         return ObjectHelper.requireNonNull(val, "IntFunction.apply(OfInt.nextInt()) returned a null value");
      }


      @Override
      public final boolean isEmpty() {
         return it == null || !it.hasNext();
      }

      @Override
      public final void clear() {
         it = null;
      }

      @Override
      public final void request(long n) {
         if (SubscriptionHelper.validate(n)) {
            if (BackpressureHelper.add(this, n) == 0L) {
               if (n == Long.MAX_VALUE) {
                  fastPath();
               } else {
                  slowPath(n);
               }
            }
         }
      }


      @Override
      public final void cancel() {
         cancelled = true;
      }

      abstract void fastPath();

      abstract void slowPath(long r);
   }

   static final class IteratorSubscription<T> extends BaseRangeSubscription<T> {
      private static final long serialVersionUID = -6022804456014692607L;

      final Subscriber<? super T> actual;

      IteratorSubscription(Subscriber<? super T> actual, PrimitiveIterator.OfInt it, IntFunction<T> intFunction) {
         super(it, intFunction);
         this.actual = actual;
      }

      @Override
      void fastPath() {
         PrimitiveIterator.OfInt it = this.it;
         IntFunction<T> intFunction = this.intFunction;
         Subscriber<? super T> a = actual;

         for (;;) {
            if (cancelled) {
               return;
            }

            T t;
            try {
               t = intFunction.apply(it.nextInt());
            } catch (Throwable ex) {
               Exceptions.throwIfFatal(ex);
               a.onError(ex);
               return;
            }

            if (cancelled) {
               return;
            }

            if (t == null) {
               a.onError(new NullPointerException("IntFunction.apply(OfInt.nextInt()) returned a null value"));
               return;
            } else {
               a.onNext(t);
            }

            if (cancelled) {
               return;
            }

            boolean b;
            try {
               b = it.hasNext();
            } catch (Throwable ex) {
               Exceptions.throwIfFatal(ex);
               a.onError(ex);
               return;
            }


            if (!b) {
               if (!cancelled) {
                  a.onComplete();
               }
               return;
            }
         }
      }

      @Override
      void slowPath(long r) {
         long e = 0L;
         PrimitiveIterator.OfInt it = this.it;
         IntFunction<T> intFunction = this.intFunction;
         Subscriber<? super T> a = actual;

         for (;;) {
            while (e != r) {
               if (cancelled) {
                  return;
               }

               T t;
               try {
                  t = intFunction.apply(it.nextInt());
               } catch (Throwable ex) {
                  Exceptions.throwIfFatal(ex);
                  a.onError(ex);
                  return;
               }

               if (cancelled) {
                  return;
               }

               if (t == null) {
                  a.onError(new NullPointerException("IntFunction.apply(OfInt.nextInt()) returned a null value"));
                  return;
               } else {
                  a.onNext(t);
               }

               if (cancelled) {
                  return;
               }

               boolean b;
               try {
                  b = it.hasNext();
               } catch (Throwable ex) {
                  Exceptions.throwIfFatal(ex);
                  a.onError(ex);
                  return;
               }

               if (!b) {
                  if (!cancelled) {
                     a.onComplete();
                  }
                  return;
               }

               e++;
            }

            r = get();
            if (e == r) {
               r = addAndGet(-e);
               if (r == 0L) {
                  return;
               }
               e = 0L;
            }
         }
      }

   }

   static final class IteratorConditionalSubscription<T> extends BaseRangeSubscription<T> {
      private static final long serialVersionUID = -6022804456014692607L;

      final ConditionalSubscriber<? super T> actual;

      IteratorConditionalSubscription(ConditionalSubscriber<? super T> actual, PrimitiveIterator.OfInt it,
            IntFunction<T> intFunction) {
         super(it, intFunction);
         this.actual = actual;
      }

      @Override
      void fastPath() {
         PrimitiveIterator.OfInt it = this.it;
         IntFunction<T> intFunction = this.intFunction;
         ConditionalSubscriber<? super T> a = actual;
         for (;;) {
            if (cancelled) {
               return;
            }

            T t;
            try {
               t = intFunction.apply(it.nextInt());
            } catch (Throwable ex) {
               Exceptions.throwIfFatal(ex);
               a.onError(ex);
               return;
            }

            if (cancelled) {
               return;
            }

            if (t == null) {
               a.onError(new NullPointerException("IntFunction.apply(OfInt.nextInt()) returned a null value"));
               return;
            } else {
               a.tryOnNext(t);
            }

            if (cancelled) {
               return;
            }

            boolean b;
            try {
               b = it.hasNext();
            } catch (Throwable ex) {
               Exceptions.throwIfFatal(ex);
               a.onError(ex);
               return;
            }

            if (!b) {
               if (!cancelled) {
                  a.onComplete();
               }
               return;
            }
         }
      }

      @Override
      void slowPath(long r) {
         long e = 0L;
         PrimitiveIterator.OfInt it = this.it;
         IntFunction<T> intFunction = this.intFunction;
         ConditionalSubscriber<? super T> a = actual;

         for (;;) {
            while (e != r) {
               if (cancelled) {
                  return;
               }

               T t;
               try {
                  t = intFunction.apply(it.nextInt());
               } catch (Throwable ex) {
                  Exceptions.throwIfFatal(ex);
                  a.onError(ex);
                  return;
               }

               if (cancelled) {
                  return;
               }

               boolean b;
               if (t == null) {
                  a.onError(new NullPointerException("IntFunction.apply(OfInt.nextInt()) returned a null value"));
                  return;
               } else {
                  b = a.tryOnNext(t);
               }

               if (cancelled) {
                  return;
               }

               boolean hasNext;
               try {
                  hasNext = it.hasNext();
               } catch (Throwable ex) {
                  Exceptions.throwIfFatal(ex);
                  a.onError(ex);
                  return;
               }

               if (!hasNext) {
                  if (!cancelled) {
                     a.onComplete();
                  }
                  return;
               }

               if (b) {
                  e++;
               }
            }

            r = get();
            if (e == r) {
               r = addAndGet(-e);
               if (r == 0L) {
                  return;
               }
               e = 0L;
            }
         }
      }

   }
}
