package org.infinispan.server.core.factories;

import static org.infinispan.server.core.transport.NettyTransport.buildEventLoop;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Factory to create netty io event loop and replace the non blocking executor with it
 *
 * @author Pedro Ruivo
 * @author William Burns
 * @since 11.0
 */
@DefaultFactoryFor(names = KnownComponentNames.NON_BLOCKING_EXECUTOR, classes = EventLoopGroup.class)
public class NettyIOFactory extends AbstractComponentFactory implements AutoInstantiableFactory {
   @Override
   public Object construct(String componentName) {
      if (componentName.equals(KnownComponentNames.NON_BLOCKING_EXECUTOR)) {
         return new BlockingTaskAwareExecutorServiceImpl(globalComponentRegistry.getComponent(EventLoopGroup.class), globalComponentRegistry.getTimeService());
      } else if (componentName.equals(EventLoopGroup.class.getName())) {
         String nodeName = globalConfiguration.transport().nodeName();
         // The NON_BLOCKING_EXECUTOR component will stop this EventLoopGroup upon shutdown
         return buildEventLoop(ProcessorInfo.availableProcessors() * 2, new DefaultThreadFactory("non-blocking-thread-netty" + (nodeName != null ? "-" + nodeName : "")));
      } else {
         throw new CacheConfigurationException("Unknown named executor " + componentName);
      }
   }
}
