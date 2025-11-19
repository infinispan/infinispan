package org.infinispan.server.core.factories;

import static org.infinispan.factories.KnownComponentNames.getDefaultThreadPrio;
import static org.infinispan.factories.KnownComponentNames.getDefaultThreads;
import static org.infinispan.factories.KnownComponentNames.shortened;
import static org.infinispan.server.core.transport.NettyTransport.buildEventLoop;

import java.util.concurrent.ThreadFactory;

import org.infinispan.commons.ThreadGroups;
import org.infinispan.commons.executors.ThreadPoolExecutorFactory;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.factories.threads.NonBlockingThreadPoolExecutorFactory;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.transport.NonRecursiveEventLoopGroup;

import io.netty.channel.EventLoopGroup;

@DefaultFactoryFor(classes = EventLoopGroup.class)
public class NettyEventLoopFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   private static final Log log = Log.getLog(NettyEventLoopFactory.class);

   @Override
   public Object construct(String componentName) {
      ThreadFactory threadFactory = globalConfiguration.nonBlockingThreadPool().threadFactory();
      if (threadFactory == null) {
         threadFactory = new io.netty.util.concurrent.DefaultThreadFactory(
               shortened(KnownComponentNames.NON_BLOCKING_EXECUTOR) + "-" + globalConfiguration.transport().nodeName(),
               true,
               getDefaultThreadPrio(KnownComponentNames.NON_BLOCKING_EXECUTOR),
               ThreadGroups.NON_BLOCKING_GROUP
         );
      } else if (!isNettyThreadFactory(threadFactory)) {
         log.useNettyThreadFactory(threadFactory.getClass());
      }

      if (threadFactory instanceof DefaultThreadFactory) {
         //not supported at the moment
         ((DefaultThreadFactory) threadFactory).useVirtualThread(false);
      }

      ThreadPoolExecutorFactory<?> tpef = globalConfiguration.nonBlockingThreadPool().threadPoolFactory();
      int threadAmount = tpef instanceof NonBlockingThreadPoolExecutorFactory ?
            ((NonBlockingThreadPoolExecutorFactory) tpef).maxThreads() :
            getDefaultThreads(KnownComponentNames.NON_BLOCKING_EXECUTOR);
      // Unfortunately, netty doesn't allow us to specify a max number of queued tasks and rejection policy at the same
      // time and the former has actually been deprecated, so we do not honor these settings when running in the server
      // which means the non blocking executor may have an unbounded queue, depending upon netty implementation
      return new NonRecursiveEventLoopGroup(buildEventLoop(threadAmount, threadFactory, "non-blocking-thread-netty"));
   }

   private static boolean isNettyThreadFactory(ThreadFactory tf) {
      return tf instanceof io.netty.util.concurrent.DefaultThreadFactory;
   }
}
