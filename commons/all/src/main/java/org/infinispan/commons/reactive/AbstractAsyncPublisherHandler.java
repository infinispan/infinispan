package org.infinispan.commons.reactive;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.functions.LongConsumer;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;

/**
 * Abstract handler that handles request and cancellation of a given non-blocking resource.
 * When additional entries are requested via {@link org.reactivestreams.Subscription#request(long)} the handler will keep
 * track of the outstanding count and invoke either {@link #sendInitialCommand(Object, int)} or
 * {@link #sendNextCommand(Object, int)} depending upon if it is the first request or subsequent.
 * This handler guarantees that there is only one outstanding request and that if the request amount is larger than the
 * batch, it will continue to send new commands one at a time until the request amount has been satisfied.
 * <p>
 * The handler processes each target via the provided {@link Supplier} one by one, exhausting all values from the
 * target, until there are no more targets left or the publishing has been cancelled by the subscriber.
 * <p>
 * When a command returns successfully either the {@link #handleInitialResponse(Object, Object)} or
 * {@link #handleNextResponse(Object, Object)} will be invoked with the response object. The returned entries can then
 * be emitted by invoking the {@link #onNext(Object)} method for each value. Note that the return of {@code onNext}
 * should be checked in case if the {@code Subscriber} has cancelled the publishing of values in the middle.
 * <p>
 * The publisher will continue to send requests to the <b>initialTarget</b> provided in the constructor. When that
 * target no longer has any more entries to retrieve the implementation should invoke {@link #targetComplete()} to signal
 * it is complete. After this is invoked the <b>supplier</b> provided in the constructor will be invoked to have the
 * next target to send to. This will repeat until the <b>supplier</b> returns <b>null</b> which is the signal to this
 * class that there are no more entries left to retrieve.
 * <p>
 * A command may also encounter a Throwable, and it is possible to customize what happens by implementing the
 * {@link #handleThrowableInResponse(Throwable, Object)} method. For example, you may want to skip the given
 * <p>The user is required to supply a <b>maxBatchSize</b> argument to the constructor. This setting will ensure that
 * this handler will never have more than this amount of entries enqueued at once. However, we may request less than
 * this batch size from the underlying target(s).
 * Each request is provided a {@code batchSize} argument and the underlying resource should adhere to,
 * failure to do so may cause an {@link OutOfMemoryError}, since entries are only emitted to the Subscriber based on the
 * requested amount, and any additional are enqueued.
 * <p>
 * This handler also supports {@link Subscription#cancel()} by extending the {@link #sendCancel(Object)} command and
 * the underlying service must be cancelled in an asynchronous and non-blocking fashion. Once cancelled this Publisher
 * will not publish additional values or requests.
 * <p>
 * Note this handler only supports a single Subscriber for the returned Publisher from {@link #startPublisher()}. Failure
 * to do so can cause multiple requests and unexpected problems.
 */
public abstract class AbstractAsyncPublisherHandler<Target, Output, InitialResponse, NextResponse> implements LongConsumer, Action {
   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   protected final int batchSize;
   protected final Supplier<Target> supplier;

   private final FlowableProcessor<Output> flowableProcessor;
   private final AtomicLong requestedAmount = new AtomicLong();

   // The current address and segments we are processing or null if another one should be acquired
   private volatile Target currentTarget;
   // whether this subscription was cancelled by a caller (means we can stop processing)
   private volatile boolean cancelled;
   // whether the initial request was already sent or not (if so then a next command is used)
   private volatile boolean alreadyCreated;

   private volatile boolean started = false;

   private final InitialBiConsumer initialBiConsumer = new InitialBiConsumer();
   private final NextBiConsumer nextBiConsumer = new NextBiConsumer();

   protected AbstractAsyncPublisherHandler(int maxBatchSize, Supplier<Target> supplier, Target firstTarget) {
      if (maxBatchSize  <= 0) {
         throw new IllegalArgumentException("maxBatchSize  must be greater than 0");
      }
      // Do not create too large of a batch, we limit it to 8196 so UnicastProcessor doesn't create too
      // large of an array
      this.batchSize = Math.min(maxBatchSize, 1 << 16);
      this.supplier = supplier;
      this.flowableProcessor = UnicastProcessor.create(this.batchSize);

      this.currentTarget = firstTarget;
   }

   public Publisher<Output> startPublisher() {
      if (!started) {
         started = true;
      } else {
         throw new IllegalStateException("Publisher was already started!");
      }
      return flowableProcessor.doOnLifecycle(RxJavaInterop.emptyConsumer(), this, this);
   }

   /**
    * This is invoked when the Subscription is cancelled
    */
   @Override
   public void run() {
      cancelled = true;
      if (alreadyCreated) {
         Target target = currentTarget;
         if (target != null) {
            sendCancel(target);
         }
      }
   }

   protected abstract void sendCancel(Target target);

   protected abstract CompletionStage<InitialResponse> sendInitialCommand(Target target, int batchSize);

   protected abstract CompletionStage<NextResponse> sendNextCommand(Target target, int batchSize);

   protected abstract long handleInitialResponse(InitialResponse response, Target target);

   protected abstract long handleNextResponse(NextResponse response, Target target);

   /**
    * Allows any implementor to handle what happens when a Throwable is encountered. By default the returned publisher
    * invokes {@link org.reactivestreams.Subscriber#onError(Throwable)} and stops processing. It is possible to
    * ignore the throwable and continue processing by invoking {@link #accept(long)} with a value of 0. It may also
    * be required to reset the {@link #currentTarget} so it is initialized to the next supplied value.
    * @param t throwable that was encountered
    * @param target the target which was invoked that caused the throwable
    */
   protected void handleThrowableInResponse(Throwable t, Target target) {
      flowableProcessor.onError(t);
   }

   /**
    * This method is invoked every time a new request is sent to the underlying publisher. We need to submit a request
    * if there is not a pending one. Whenever requestedAmount is a number greater than 0, that means we must submit or
    * there is a pending one.
    * @param count request count
    */
   @Override
   public void accept(long count) {
      if (shouldSubmit(count)) {
         if (checkCancelled()) {
            return;
         }

         // Find which address and segments we still need to retrieve - when the supplier returns null that means
         // we don't need to do anything else (normal termination state)
         Target target = currentTarget;
         if (target == null) {
            alreadyCreated = false;
            target = supplier.get();
            if (target == null) {
               if (log.isTraceEnabled()) {
                  log.tracef("Completing processor %s", flowableProcessor);
               }
               flowableProcessor.onComplete();
               return;
            } else {
               currentTarget = target;
            }
         }

         try {
            if (alreadyCreated) {
               CompletionStage<NextResponse> stage = sendNextCommand(target, batchSize);
               stage.whenComplete(nextBiConsumer);
            } else {
               alreadyCreated = true;
               CompletionStage<InitialResponse> stage = sendInitialCommand(target, batchSize);
               stage.whenComplete(initialBiConsumer);
            }
         } catch (Throwable t) {
            handleThrowableInResponse(t, target);
         }
      }
   }

   private class InitialBiConsumer extends ResponseConsumer<InitialResponse> {
      @Override
      long handleResponse(InitialResponse response, Target target) {
         return handleInitialResponse(response, target);
      }
   }

   private class NextBiConsumer extends ResponseConsumer<NextResponse> {
      @Override
      long handleResponse(NextResponse response, Target target) {
         return handleNextResponse(response, target);
      }
   }

   private abstract class ResponseConsumer<Type> implements BiConsumer<Type, Throwable> {
      @Override
      public void accept(Type response, Throwable throwable) {
         if (throwable != null) {
            try {
               handleThrowableInResponse(throwable, currentTarget);
            } catch (Throwable innerT) {
               // If there was an exception while processing an exception immediately set it on the flowable
               flowableProcessor.onError(innerT);
            }
            return;
         }
         try {
            long produced = handleResponse(response, currentTarget);

            AbstractAsyncPublisherHandler.this.accept(-produced);
         } catch (Throwable innerT) {
            handleThrowableInResponse(innerT, currentTarget);
         }
      }

      abstract long handleResponse(Type response, Target target);
   }

   /**
    * Method that should be called for each emitted output value. The returned boolean is whether the handler should
    * continue publishing values or not.
    * @param value value emit to the publisher
    * @return whether to continue emitting values
    */
   protected boolean onNext(Output value) {
      if (checkCancelled()) {
         return false;
      }
      flowableProcessor.onNext(value);
      return true;
   }

   /**
    * Method to invoke when a given target is found to have been completed and the next target should be used
    */
   protected void targetComplete() {
      // Setting to null will force the next invocation of accept to retrieve the next target if available
      currentTarget = null;
   }

   private boolean shouldSubmit(long count) {
      while (true) {
         long prev = requestedAmount.get();
         long newValue = prev + count;
         if (requestedAmount.compareAndSet(prev, newValue)) {
            // This ensures that only a single submission can be done at one time
            // It will only submit if there were none prior (prev <= 0) or if it is the current one (count <= 0).
            return newValue > 0 && (prev <= 0 || count <= 0);
         }
      }
   }

   /**
    * This method returns whether this subscription has been cancelled
    */
   // This method doesn't have to be protected by requestors, but there is no reason for a method who doesn't have
   // the requestors "lock" to invoke this
   protected boolean checkCancelled() {
      if (cancelled) {
         if (log.isTraceEnabled()) {
            log.tracef("Subscription %s was cancelled, terminating early", this);
         }
         return true;
      }
      return false;
   }
}
