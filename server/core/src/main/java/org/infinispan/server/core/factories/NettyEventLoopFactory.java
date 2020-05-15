package org.infinispan.server.core.factories;

import static org.infinispan.factories.KnownComponentNames.getDefaultThreadPrio;
import static org.infinispan.factories.KnownComponentNames.getDefaultThreads;
import static org.infinispan.factories.KnownComponentNames.shortened;
import static org.infinispan.server.core.transport.NettyTransport.buildEventLoop;

import java.util.concurrent.ThreadFactory;

import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.factories.threads.NonBlockingThreadFactory;
import org.infinispan.factories.threads.NonBlockingThreadPoolExecutorFactory;
import org.infinispan.server.core.transport.NonRecursiveEventLoopGroup;

import io.netty.channel.EventLoopGroup;

@DefaultFactoryFor(classes = EventLoopGroup.class)
public class NettyEventLoopFactory extends AbstractComponentFactory implements AutoInstantiableFactory {
   @Override
   public Object construct(String componentName) {
      ThreadFactory threadFactory = globalConfiguration.nonBlockingThreadPool().threadFactory();
      if (threadFactory == null) {
         threadFactory = new NonBlockingThreadFactory("ISPN-non-blocking-thread-group",
               getDefaultThreadPrio(KnownComponentNames.NON_BLOCKING_EXECUTOR), DefaultThreadFactory.DEFAULT_PATTERN,
               globalConfiguration.transport().nodeName(), shortened(KnownComponentNames.NON_BLOCKING_EXECUTOR));
      }

      ThreadPoolExecutorFactory<?> tpef = globalConfiguration.nonBlockingThreadPool().threadPoolFactory();
      int threadAmount = tpef instanceof NonBlockingThreadPoolExecutorFactory ?
            ((NonBlockingThreadPoolExecutorFactory) tpef).maxThreads() :
            getDefaultThreads(KnownComponentNames.NON_BLOCKING_EXECUTOR);
      // Unfortunately, netty doesn't allow us to specify a max number of queued tasks and rejection policy at the same
      // time and the former has actually been deprecated, so we do not honor these settings when running in the server
      // which means the non blocking executor may have an unbounded queue, depending upon netty implementation
      return new NonRecursiveEventLoopGroup(buildEventLoop(threadAmount, threadFactory,
            "non-blocking-thread-netty"));
   }
}
