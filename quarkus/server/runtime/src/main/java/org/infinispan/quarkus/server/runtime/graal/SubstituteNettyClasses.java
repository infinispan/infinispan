package org.infinispan.quarkus.server.runtime.graal;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Delete;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.core.transport.NettyTransport;

import java.util.concurrent.ThreadFactory;

public class SubstituteNettyClasses {
}

@Delete
@TargetClass(className = "org.infinispan.server.core.transport.NativeTransport")
final class Delete_org_infinispan_server_core_transport_NativeTransport { }

@TargetClass(NettyTransport.class)
final class Substitute_NettyTransport {
   @Alias
   static private Log log;
   @Alias
   private ProtocolServerConfiguration configuration;

   @Substitute
   private Class<? extends ServerChannel> getServerSocketChannel() {
      Class<? extends ServerChannel> channel = NioServerSocketChannel.class;
      log.createdSocketChannel(channel.getName(), configuration.toString());
      return channel;
   }

   @Substitute
   public static MultithreadEventLoopGroup buildEventLoop(int nThreads, ThreadFactory threadFactory,
         String configuration) {
      MultithreadEventLoopGroup eventLoop = new NioEventLoopGroup(nThreads, threadFactory);
      log.createdNettyEventLoop(eventLoop.getClass().getName(), configuration);
      return eventLoop;
   }
}

/*
 * Reinstate once we include the Hot Rod client
 *
@Delete
@TargetClass(className = "org.infinispan.client.hotrod.impl.transport.netty.NativeTransport")
final class Delete_org_infinispan_client_hotrod_impl_transport_netty_NativeTransport { }
*/
