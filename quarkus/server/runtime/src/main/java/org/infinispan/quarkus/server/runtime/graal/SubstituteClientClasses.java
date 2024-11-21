package org.infinispan.quarkus.server.runtime.graal;

import java.util.concurrent.ExecutorService;

import javax.management.ObjectName;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.commons.marshall.Marshaller;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Delete;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

@Substitute
@TargetClass(className = "org.infinispan.client.hotrod.impl.transport.netty.DefaultTransportFactory")
final class SubstituteTransportHelper {

    @Substitute
    public Class<? extends SocketChannel> socketChannelClass() {
        return NioSocketChannel.class;
    }

    @Substitute
    public EventLoopGroup createEventLoopGroup(int maxExecutors, ExecutorService executorService) {
        return new NioEventLoopGroup(maxExecutors, executorService);
    }
}

@TargetClass(RemoteCacheManager.class)
final class SubstituteRemoteCacheManager {
    @Alias
    private Marshaller marshaller;
    @Alias
    private Configuration configuration;
    @Alias
    protected OperationDispatcher dispatcher;

    @Substitute
    private void initRemoteCache(InternalRemoteCache<?, ?> remoteCache) {
        remoteCache.init(configuration, dispatcher);
    }

    @Substitute
    private void registerMBean() {
    }

    @Substitute
    private void unregisterMBean() {

    }
}

@TargetClass(RemoteCacheImpl.class)
final class SubstituteRemoteCacheImpl {
    @Delete
    private ObjectName mbeanObjectName;

    @Substitute
    private void registerMBean(ObjectName jmxParent) {
    }

    @Substitute
    private void unregisterMBean() {
    }

    // Sadly this method is public, so technically a user could get a Runtime error if they were referencing
    // it before - but it is the only way to make graal happy
    @Delete
    public void init(Configuration configuration, OperationDispatcher dispatcher, ObjectName jmxParent) {
    }
}

// TODO sort out duplication with quarkus infinispan-client extension
public class SubstituteClientClasses {
}
