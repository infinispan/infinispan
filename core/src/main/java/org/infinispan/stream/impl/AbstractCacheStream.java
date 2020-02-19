package org.infinispan.stream.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.CacheStream;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.stream.impl.intops.FlatMappingOperation;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.stream.impl.intops.MappingOperation;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * Abstract stream that provides all of the common functionality required for all types of Streams including the various
 * primitive types.
 * @param <Original> the original type of the underlying stream - normally CacheEntry or Object
 * @param <T> The type returned by the stream
 * @param <S> The stream interface
 */
public abstract class AbstractCacheStream<Original, T, S extends BaseStream<T, S>, S2 extends S> implements BaseStream<T, S> {
   protected final Queue<IntermediateOperation> intermediateOperations;
   protected final Address localAddress;
   protected final DistributionManager dm;
   protected final Supplier<CacheStream<Original>> supplier;
   protected final ClusterPublisherManager cpm;
   protected final Executor executor;
   protected final ComponentRegistry registry;
   protected final PartitionHandlingManager partition;
   protected final KeyPartitioner keyPartitioner;
   protected final StateTransferLock stateTransferLock;
   protected final boolean includeLoader;
   protected final Function<? super Original, ?> toKeyFunction;
   protected final InvocationContext invocationContext;

   protected Runnable closeRunnable = null;

   protected Boolean parallelDistribution;
   protected boolean parallel;
   protected boolean rehashAware = true;

   protected Set<?> keysToFilter;
   protected IntSet segmentsToFilter;

   protected int distributedBatchSize;

   protected Consumer<Supplier<PrimitiveIterator.OfInt>> segmentCompletionListener;

   protected IteratorOperation iteratorOperation = IteratorOperation.NO_MAP;

   protected long timeout = 30;
   protected TimeUnit timeoutUnit = TimeUnit.SECONDS;

   protected AbstractCacheStream(Address localAddress, boolean parallel, DistributionManager dm, InvocationContext ctx,
           Supplier<CacheStream<Original>> supplier, boolean includeLoader, int distributedBatchSize, Executor executor,
         ComponentRegistry registry, Function<? super Original, ?> toKeyFunction) {
      this.localAddress = localAddress;
      this.parallel = parallel;
      this.dm = dm;
      this.invocationContext = ctx;
      this.supplier = supplier;
      this.includeLoader = includeLoader;
      this.distributedBatchSize = distributedBatchSize;
      this.executor = executor;
      this.registry = registry;
      this.toKeyFunction = toKeyFunction;
      this.partition = registry.getComponent(PartitionHandlingManager.class);
      this.keyPartitioner = registry.getComponent(KeyPartitioner.class);
      this.stateTransferLock = registry.getComponent(StateTransferLock.class);
      this.cpm = registry.getComponent(ClusterPublisherManager.class);
      intermediateOperations = new ArrayDeque<>();
   }

   protected AbstractCacheStream(AbstractCacheStream<Original, T, S, S2> other) {
      this.intermediateOperations = other.intermediateOperations;
      this.localAddress = other.localAddress;
      this.dm = other.dm;
      this.invocationContext = other.invocationContext;
      this.supplier = other.supplier;
      this.includeLoader = other.includeLoader;
      this.executor = other.executor;
      this.registry = other.registry;
      this.toKeyFunction = other.toKeyFunction;
      this.partition = other.partition;
      this.keyPartitioner = other.keyPartitioner;
      this.stateTransferLock = other.stateTransferLock;
      this.cpm = other.cpm;

      this.closeRunnable = other.closeRunnable;

      this.parallel = other.parallel;

      this.parallelDistribution = other.parallelDistribution;
      this.rehashAware = other.rehashAware;

      this.keysToFilter = other.keysToFilter;
      this.segmentsToFilter = other.segmentsToFilter;

      this.distributedBatchSize = other.distributedBatchSize;

      this.segmentCompletionListener = other.segmentCompletionListener;

      this.iteratorOperation = other.iteratorOperation;

      this.timeout = other.timeout;
      this.timeoutUnit = other.timeoutUnit;
   }

   protected abstract Log getLog();

   protected S2 addIntermediateOperation(IntermediateOperation<T, S, T, S> intermediateOperation) {
      intermediateOperation.handleInjection(registry);
      addIntermediateOperation(intermediateOperations, intermediateOperation);
      return unwrap();
   }

   protected void addIntermediateOperationMap(IntermediateOperation<T, S, ?, ?> intermediateOperation) {
      intermediateOperation.handleInjection(registry);
      addIntermediateOperation(intermediateOperations, intermediateOperation);
   }

   protected void addIntermediateOperation(Queue<IntermediateOperation> intermediateOperations,
           IntermediateOperation<T, S, ?, ?> intermediateOperation) {
      intermediateOperations.add(intermediateOperation);
   }

   protected abstract S2 unwrap();

   @Override
   public boolean isParallel() {
      return parallel;
   }

   @Override
   public S2 sequential() {
      parallel = false;
      return unwrap();
   }

   @Override
   public S2 parallel() {
      parallel = true;
      return unwrap();
   }

   @Override
   public S2 unordered() {
      // This by default is always unordered
      return unwrap();
   }

   @Override
   public S2 onClose(Runnable closeHandler) {
      if (this.closeRunnable == null) {
         this.closeRunnable = closeHandler;
      } else {
         this.closeRunnable = Util.composeWithExceptions(this.closeRunnable, closeHandler);
      }
      return unwrap();
   }

   @Override
   public void close() {
      if (closeRunnable != null) {
         closeRunnable.run();
      }
   }

   <R> R performPublisherOperation(Function<? super Publisher<T>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      Function usedTransformer;
      if (intermediateOperations.isEmpty()) {
         usedTransformer = transformer;
      } else {
         usedTransformer = new CacheStreamIntermediateReducer(intermediateOperations, transformer);
      }

      DeliveryGuarantee guarantee = rehashAware ? DeliveryGuarantee.EXACTLY_ONCE : DeliveryGuarantee.AT_MOST_ONCE;
      CompletionStage<R> stage;
      if (toKeyFunction == null) {
         stage = cpm.keyReduction(parallel, segmentsToFilter, keysToFilter, invocationContext, includeLoader, guarantee,
               usedTransformer, finalizer);
      } else {
         stage = cpm.entryReduction(parallel, segmentsToFilter, keysToFilter, invocationContext, includeLoader, guarantee,
               usedTransformer, finalizer);
      }
      return CompletionStages.join(stage);
   }

   protected boolean isPrimaryOwner(ConsistentHash ch, Object key) {
      return localAddress.equals(ch.locatePrimaryOwnerForSegment(keyPartitioner.getSegment(key)));
   }

   enum IteratorOperation {
      NO_MAP,
      MAP {
         /**
          * Function to be used to unwrap an entry. If this is null, then no wrapping is required
          * @return a function to apply
          */
         @Override
         public <In, Out> Function<In, Out> getFunction() {
            // Map should be wrap entry in KVP<Key, Result(s)> so we have to unwrap those result(s)
            return e -> ((KeyValuePair<?, Out>) e).getValue();
         }
      },
      FLAT_MAP
      ;

      public <In, Out> Function<In, Out> getFunction() {
         // There is no unwrapping required as we just have the CacheEntry directly
         return null;
      }
   }

   static class MapHandler<OutputType, OutputStream extends BaseStream<OutputType, OutputStream>>
         implements MappingOperation<Object, Stream<Object>, OutputType, OutputStream> {
      final Iterable<IntermediateOperation> intermediateOperations;
      final Function<Object, ?> toKeyFunction;

      MapHandler(Iterable<IntermediateOperation> intermediateOperations, Function<Object, ?> toKeyFunction) {
         this.intermediateOperations = intermediateOperations;
         this.toKeyFunction = toKeyFunction;
      }

      @Override
      public OutputStream perform(Stream<Object> cacheEntryStream) {
         ByRef<Object> key = new ByRef<>(null);
         BaseStream stream = cacheEntryStream.peek(e -> key.set(toKeyFunction.apply(e)));
         for (IntermediateOperation intermediateOperation : intermediateOperations) {
            stream = intermediateOperation.perform(stream);
         }
         // We assume the resulting stream contains objects (this is because we also box all primitives). If this
         // changes we need to change this code to handle primitives as well (most likely add MAP_DOUBLE etc.)
         return (OutputStream) ((Stream) stream).map(r -> new KeyValuePair<>(key.get(), r));
      }

      @Override
      public Flowable<OutputType> mapFlowable(Flowable<Object> input) {
         // This is not used except for iteration - which is not yet supported with distributed publisher
         throw new UnsupportedOperationException("Not implemented");
      }
   }

   static class FlatMapHandler<OutputType, OutputStream extends BaseStream<OutputType, OutputStream>>
         extends MapHandler<OutputType, OutputStream> {
      FlatMapHandler(Iterable<IntermediateOperation> intermediateOperations, Function<Object, ?> toKeyFunction) {
         super(intermediateOperations, toKeyFunction);
      }

      @Override
      public OutputStream perform(Stream<Object> cacheEntryStream) {
         ByRef<Object> key = new ByRef<>(null);
         BaseStream stream = cacheEntryStream.peek(e -> key.set(toKeyFunction.apply(e)));

         Iterator<IntermediateOperation> iter = intermediateOperations.iterator();
         while (iter.hasNext()) {
            IntermediateOperation intermediateOperation = iter.next();
            if (intermediateOperation instanceof FlatMappingOperation) {
               // We have to copy this over to list as we have to iterate upon it for every entry
               List<IntermediateOperation> remainingOps = new ArrayList<>();
               iter.forEachRemaining(remainingOps::add);
               // If we ran into our first flat map operation - then we have to create a flattened stream
               // where instead of having multiple elements in the stream we have 1 that is composed of
               // a KeyValuePair that has the key pointing to the resulting flatMap stream
               Stream<BaseStream> wrappedStream = ((FlatMappingOperation) intermediateOperation).map(stream);
               stream = wrappedStream.map(s -> {
                  for (IntermediateOperation innerIntOp : remainingOps) {
                     s = innerIntOp.perform(s);
                  }
                  return new KeyValuePair<>(key.get(), ((Stream) s).collect(Collectors.toList()));
               });
            } else {
               stream = intermediateOperation.perform(stream);
            }
         }
         return (OutputStream) stream;
      }
   }

   /**
    * Given two SegmentCompletionListener, return a SegmentCompletionListener that
    * executes both in sequence, even if the first throws an exception, and if both
    * throw exceptions, add any exceptions thrown by the second as suppressed
    * exceptions of the first.
    */
   protected static Consumer<Supplier<PrimitiveIterator.OfInt>> composeWithExceptions(Consumer<Supplier<PrimitiveIterator.OfInt>> a,
         Consumer<Supplier<PrimitiveIterator.OfInt>> b) {
      return (segments) -> {
         try {
            a.accept(segments);
         }
         catch (Throwable e1) {
            try {
               b.accept(segments);
            }
            catch (Throwable e2) {
               try {
                  e1.addSuppressed(e2);
               } catch (Throwable ignore) {}
            }
            throw e1;
         }
         b.accept(segments);
      };
   }

   public static class MapOpsExternalizer extends AbstractExternalizer<IntermediateOperation> {
      static final int MAP = 0;
      static final int FLATMAP = 1;
      private final Map<Class<?>, Integer> numbers = new HashMap<>(2);

      public MapOpsExternalizer() {
         numbers.put(MapHandler.class, MAP);
         numbers.put(FlatMapHandler.class, FLATMAP);
      }

      @Override
      public Integer getId() {
         return Ids.STREAM_MAP_OPS;
      }

      @Override
      public Set<Class<? extends IntermediateOperation>> getTypeClasses() {
         return Util.asSet(MapHandler.class, FlatMapHandler.class);
      }

      @Override
      public void writeObject(ObjectOutput output, IntermediateOperation object) throws IOException {
         int number = numbers.getOrDefault(object.getClass(), -1);
         output.write(number);
         switch (number) {
            case MAP:
            case FLATMAP:
               output.writeObject(((MapHandler) object).intermediateOperations);
               output.writeObject(((MapHandler) object).toKeyFunction);
               break;
            default:
               throw new IllegalArgumentException("Unsupported number " + number + " found for class: " + object.getClass());
         }
      }

      @Override
      public IntermediateOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int number = input.readUnsignedByte();
         switch (number) {
            case MAP:
               return new MapHandler<>((Iterable<IntermediateOperation>) input.readObject(), (Function) input.readObject());
            case FLATMAP:
               return new FlatMapHandler<>((Iterable<IntermediateOperation>) input.readObject(), (Function) input.readObject());
            default:
               throw new IllegalArgumentException("Unsupported number " + number + " found!");
         }
      }
   }
}
