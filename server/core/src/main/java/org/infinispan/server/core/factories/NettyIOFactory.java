package org.infinispan.server.core.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.server.core.transport.NettyNonBlockingManager;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;
import org.infinispan.util.concurrent.NonBlockingManager;

import io.netty.channel.EventLoopGroup;

/**
 * Factory to create netty io event loop and replace the non blocking executor with it
 *
 * @author Pedro Ruivo
 * @author William Burns
 * @since 11.0
 */
@DefaultFactoryFor(names = KnownComponentNames.NON_BLOCKING_EXECUTOR, classes = NonBlockingManager.class)
public class NettyIOFactory extends AbstractComponentFactory implements AutoInstantiableFactory {
   @Inject
   protected BasicComponentRegistry basicComponentRegistry;

   @Override
   public Object construct(String componentName) {
      if (componentName.equals(KnownComponentNames.NON_BLOCKING_EXECUTOR)) {
         ComponentRef<EventLoopGroup> ref = basicComponentRegistry.getComponent(EventLoopGroup.class);
         // This means our event loop can't have a cyclical dependency on us otherwise we will get stuck
         EventLoopGroup runningEventLoopGroup = ref.running();
         return new BlockingTaskAwareExecutorServiceImpl(runningEventLoopGroup, globalComponentRegistry.getTimeService());
      } else if (componentName.equals(NonBlockingManager.class.getName())) {
         return new NettyNonBlockingManager();
      } else {
         throw new CacheConfigurationException("Unknown named component " + componentName);
      }
   }

}
