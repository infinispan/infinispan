package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;

/**
 * An HotRod operation that span across multiple remote nodes concurrently (like getAll / putAll).
 *
 * @author Guillaume Darmont / guillaume@dropinocean.com
 */
public abstract class HotRodBulkOperation<Input, Output, Op extends HotRodOperation<Output>> {
   protected final DataFormat dataFormat;
   protected final Function<Input, Op> opFunction;

   public HotRodBulkOperation(DataFormat dataFormat, Function<Input, Op> opFunction) {
      this.dataFormat = dataFormat;
      this.opFunction = opFunction;
   }

   public final CompletionStage<Output> executeOperations(Function<Object, SocketAddress> routingFunction,
                                                        BiFunction<Op, SocketAddress, CompletionStage<Output>> invoker) {
      Collection<Output> results = Collections.synchronizedList(new ArrayList<>());
      AggregateCompletionStage<Collection<Output>> acs = CompletionStages.aggregateCompletionStage(results);

      Map<SocketAddress, Op> map = gatherOperations(routingFunction);

      map.forEach((addr, op) -> acs.dependsOn(invoker.apply(op, addr)
            .thenAccept(results::add)));

      return reduce(acs.freeze());
   }

   protected SocketAddress getAddressForKey(Object key, byte[] keyBytes, Function<Object, SocketAddress> routingFunction) {
      return routingFunction.apply(dataFormat.isObjectStorage() ? key : keyBytes);
   }

   protected abstract Map<SocketAddress, Op> gatherOperations(Function<Object, SocketAddress> routingFunction);

   public abstract CompletionStage<Output> reduce(CompletionStage<Collection<Output>> stage);
}
