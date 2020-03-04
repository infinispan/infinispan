package org.infinispan.server.hotrod.test;

import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.host;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readString;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readUnsignedLong;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeUnsignedLong;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.commons.util.Either;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.commons.test.TestResourceTracker;
import org.testng.annotations.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.util.concurrent.DefaultThreadFactory;

@Test(groups = "functional", testName = "server.hotrod.test.HotRodPipeTest")
public class HotRodPipeTest extends SingleCacheManagerTest {

   HotRodServer server;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      TestCacheManagerFactory.amendTransport(gcb);
      return TestCacheManagerFactory.createServerModeCacheManager(gcb);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      server = HotRodTestingUtil.startHotRodServer(cacheManager);
   }

   @Override
   protected void teardown() {
      log.debug("Killing Hot Rod server");
      killServer(server);
   }

   public void testPipeRequests() throws InterruptedException {
      final int numPipeReqs = 10_000;
      BatchingClient client = new BatchingClient(server.getPort());
      try {
         client.start();
         client.writeN(numPipeReqs);
         eventuallyEquals(numPipeReqs, () -> {
            Either<List<String>, Integer> either = client.readN();
            switch (either.type()) {
               case LEFT:
                  throw new AssertionError(either.left().get(0));
               case RIGHT:
                  return either.right();
               default:
                  throw new IllegalStateException("Either can only be left or right");
            }
         });
      } finally {
         client.stop();
      }
   }

   static final class BatchingClient {

      final EventLoopGroup group;
      final int port;

      Channel ch;

      BatchingClient(int port) {
         this.port = port;
         DefaultThreadFactory threadFactory = new DefaultThreadFactory(TestResourceTracker.getCurrentTestShortName());
         group = new NioEventLoopGroup(0, threadFactory);
      }

      void start() {
         Bootstrap b = new Bootstrap();
         b.group(group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .handler(new ChannelInitializer<Channel>() {
               @Override
               protected void initChannel(Channel ch) throws Exception {
                  ChannelPipeline p = ch.pipeline();
                  p.addLast(new BatchingDecoder());
                  p.addLast(new BatchingEncoder());
                  p.addLast(new BatchingClientHandler());
               }
            });

         try {
            ChannelFuture f = b.connect(host(), port).sync();
            ch = f.channel();
         } catch (InterruptedException e) {
            throw new AssertionError(e);
         }
      }

      void stop() throws InterruptedException {
         group.shutdownGracefully().await(10, TimeUnit.SECONDS);
      }

      void writeN(int n) {
         ch.writeAndFlush(n);
      }

      Either<List<String>, Integer> readN() {
         BatchingClientHandler last = (BatchingClientHandler) ch.pipeline().last();
         return last.errors.isEmpty()
               ? Either.newRight(last.n)
               : Either.newLeft(last.errors);
      }

      private static final class BatchingEncoder extends MessageToByteEncoder {
         @Override
         protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
            int n = (int) msg;
            IntStream.range(0, n).forEach(i -> {
               out.writeByte(0xA0);          // magic
               writeUnsignedLong(i, out);    // message id
               out.writeByte(0x19);          // version
               out.writeByte(0x01);          // op code
               out.writeByte(0x00);          // cache name empty
               out.writeByte(0x00);          // flags
               out.writeByte(0x03);          // client intelligence
               out.writeByte(0x00);          // topology id
               out.writeBytes(new byte[] {   // operation parameters
                  0x03, 0x31, 0x30, 0x30, 0x77, 0x03, 0x31, 0x30, 0x30
               });
            });
         }
      }

      private static final class BatchingDecoder extends ReplayingDecoder {

         @Override
         protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            in.readUnsignedByte();                 // magic byte
            long id = readUnsignedLong(in);        // message id
            short op = in.readUnsignedByte();      // op code
            in.readUnsignedByte();                 // status code
            in.readUnsignedByte();                 // topology marker

            switch (op) {
               case 0x02: // normal response
                  out.add(id);
                  break;
               case 0x50: // error response
                  String error = readString(in);
                  out.add(error);
                  break;
            }
         }

      }

      private static final class BatchingClientHandler extends SimpleChannelInboundHandler<Object> {
         int n;
         List<String> errors = new ArrayList<>();

         @Override
         protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof String) {
               errors.add((String) msg);
            } else {
               n++;
            }
         }
      }

   }

}
