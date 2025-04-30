package org.infinispan.server.core.test;

import io.netty.util.concurrent.FastThreadLocalThread;
import reactor.blockhound.BlockHound;
import reactor.blockhound.integration.BlockHoundIntegration;

/**
 * This is copied from https://github.com/netty/netty/blob/9ef29348563bdffdbe37966da48316f8742755a8/common/src/main/java/io/netty/util/internal/Hidden.java
 * as we need to ensure the blockhound exceptions are available for downstream builds.
 * <p>
 * Make sure to update the SHA above when making changes to ensure we have the most up to date Netty Hidden file contents.
 */
class NettyHidden {
   /**
    * This class integrates Netty with BlockHound.
    * <p>
    * It is public but only because of the ServiceLoader's limitations
    * and SHOULD NOT be considered a public API.
    */
   public static final class NettyBlockHoundIntegration implements BlockHoundIntegration {

      @Override
      public void applyTo(BlockHound.Builder builder) {
         builder.allowBlockingCallsInside(
               "io.netty.channel.nio.NioEventLoop",
               "handleLoopException"
         );

         builder.allowBlockingCallsInside(
               "io.netty.channel.kqueue.KQueueEventLoop",
               "handleLoopException"
         );

         builder.allowBlockingCallsInside(
               "io.netty.channel.epoll.EpollEventLoop",
               "handleLoopException"
         );

         builder.allowBlockingCallsInside(
               "io.netty.util.HashedWheelTimer",
               "start"
         );

         builder.allowBlockingCallsInside(
               "io.netty.util.HashedWheelTimer",
               "stop"
         );

         builder.allowBlockingCallsInside(
               "io.netty.util.HashedWheelTimer$Worker",
               "waitForNextTick"
         );

         builder.allowBlockingCallsInside(
               "io.netty.util.concurrent.SingleThreadEventExecutor",
               "confirmShutdown"
         );

         builder.allowBlockingCallsInside(
               "io.netty.buffer.PoolArena",
               "lock"
         );

         builder.allowBlockingCallsInside(
               "io.netty.buffer.PoolSubpage",
               "lock"
         );

         builder.allowBlockingCallsInside(
               "io.netty.buffer.PoolChunk",
               "allocateRun"
         );

         builder.allowBlockingCallsInside(
               "io.netty.buffer.PoolChunk",
               "free"
         );

         builder.allowBlockingCallsInside(
               "io.netty.buffer.AdaptivePoolingAllocator$1",
               "initialValue"
         );

         builder.allowBlockingCallsInside(
               "io.netty.buffer.AdaptivePoolingAllocator$1",
               "onRemoval"
         );

         builder.allowBlockingCallsInside(
               "io.netty.handler.ssl.SslHandler",
               "handshake"
         );

         builder.allowBlockingCallsInside(
               "io.netty.handler.ssl.SslHandler",
               "runAllDelegatedTasks"
         );
         builder.allowBlockingCallsInside(
               "io.netty.handler.ssl.SslHandler",
               "runDelegatedTasks"
         );
         builder.allowBlockingCallsInside(
               "io.netty.util.concurrent.GlobalEventExecutor",
               "takeTask");

         builder.allowBlockingCallsInside(
               "io.netty.util.concurrent.GlobalEventExecutor",
               "addTask");

         builder.allowBlockingCallsInside(
               "io.netty.util.concurrent.SingleThreadEventExecutor",
               "takeTask");

         builder.allowBlockingCallsInside(
               "io.netty.util.concurrent.SingleThreadEventExecutor",
               "addTask");

         builder.allowBlockingCallsInside(
               "io.netty.handler.ssl.ReferenceCountedOpenSslClientContext$ExtendedTrustManagerVerifyCallback",
               "verify");

         builder.allowBlockingCallsInside(
               "io.netty.handler.ssl.JdkSslContext$Defaults",
               "init");

         // Let's whitelist SSLEngineImpl.unwrap(...) for now as it may fail otherwise for TLS 1.3.
         // See https://mail.openjdk.java.net/pipermail/security-dev/2020-August/022271.html
         builder.allowBlockingCallsInside(
               "sun.security.ssl.SSLEngineImpl",
               "unwrap");

         builder.allowBlockingCallsInside(
               "sun.security.ssl.SSLEngineImpl",
               "wrap");

         builder.allowBlockingCallsInside(
               "io.netty.resolver.dns.UnixResolverDnsServerAddressStreamProvider",
               "parse");

         builder.allowBlockingCallsInside(
               "io.netty.resolver.dns.UnixResolverDnsServerAddressStreamProvider",
               "parseEtcResolverSearchDomains");

         builder.allowBlockingCallsInside(
               "io.netty.resolver.dns.UnixResolverDnsServerAddressStreamProvider",
               "parseEtcResolverOptions");

         builder.allowBlockingCallsInside(
               "io.netty.resolver.HostsFileEntriesProvider$ParserImpl",
               "parse");

         builder.allowBlockingCallsInside(
               "io.netty.util.NetUtil$SoMaxConnAction",
               "run");

         builder.allowBlockingCallsInside("io.netty.util.internal.ReferenceCountUpdater",
               "retryRelease0");

         builder.allowBlockingCallsInside("io.netty.util.internal.PlatformDependent", "createTempFile");
         builder.nonBlockingThreadPredicate(p -> thread -> p.test(thread) ||
               thread instanceof FastThreadLocalThread &&
                     !((FastThreadLocalThread) thread).permitBlockingCalls());
      }

      @Override
      public int compareTo(BlockHoundIntegration o) {
         return 0;
      }
   }
}
