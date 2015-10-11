package org.infinispan.interceptors.distribution;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.read.AbstractCloseableIteratorCollection;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ForwardingCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.base.SequentialInterceptor;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.DistributedCacheStream;
import org.infinispan.stream.impl.RemovableCloseableIterator;
import org.infinispan.stream.impl.RemovableIterator;
import org.infinispan.stream.impl.tx.TxClusterStreamManager;
import org.infinispan.stream.impl.tx.TxDistributedCacheStream;
import org.infinispan.transaction.impl.LocalTransaction;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.infinispan.factories.KnownComponentNames.ASYNC_OPERATIONS_EXECUTOR;

/**
 * Interceptor that handles bulk entrySet and keySet commands when using in a distributed environment.  This
 * interceptor produces backing collections for either method and a distributed stream for either which leverages
 * distributed processing through the cluster.
 * @param <K> The key type of entries
 * @param <V> The value type of entries
 * @deprecated Since 8.1, use {@link org.infinispan.interceptors.sequential.DistributionBulkInterceptor} instead.
 */
@Deprecated
public class DistributionBulkInterceptor<K, V> extends CommandInterceptor {
   @Override
   public Class<? extends SequentialInterceptor> getSequentialInterceptor() {
      return org.infinispan.interceptors.sequential.DistributionBulkInterceptor.class;
   }
}
