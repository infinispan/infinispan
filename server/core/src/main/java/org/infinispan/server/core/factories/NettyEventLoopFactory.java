package org.infinispan.server.core.factories;

import static org.infinispan.server.core.transport.NettyTransport.buildEventLoop;

import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.server.core.transport.NonRecursiveEventLoopGroup;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

@DefaultFactoryFor(classes = EventLoopGroup.class)
public class NettyEventLoopFactory extends AbstractComponentFactory implements AutoInstantiableFactory {
   @Override
   public Object construct(String componentName) {
      String nodeName = globalConfiguration.transport().nodeName();
      return new NonRecursiveEventLoopGroup(buildEventLoop(ProcessorInfo.availableProcessors() * 2,
            new DefaultThreadFactory("non-blocking-thread-netty" + (nodeName != null ? "-" + nodeName : ""))));
   }
}
