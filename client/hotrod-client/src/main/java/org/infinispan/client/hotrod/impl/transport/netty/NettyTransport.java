package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.impl.AbstractTransport;
import org.infinispan.client.hotrod.impl.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportException;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class NettyTransport extends AbstractTransport {

   private InetSocketAddress serverAddress;
   private Channel channel;
   private ChannelFuture lastWrite;

   private HotrodClientDecoder decoder = new HotrodClientDecoder();

   public NettyTransport(InetSocketAddress serverAddress) {
      this.serverAddress = serverAddress;
      init();
   }

   private void init() {
      // Configure the client.
      ClientBootstrap bootstrap = new ClientBootstrap(
            new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));

      // Set up the event pipeline factory.
      bootstrap.setPipelineFactory(new HotrodClientPipelaneFactory(decoder));

      // Start the connection attempt.
      ChannelFuture future = bootstrap.connect(serverAddress);

      // Wait until the connection attempt succeeds or fails.
      channel = future.awaitUninterruptibly().getChannel();
      if (!future.isSuccess()) {
         bootstrap.releaseExternalResources();
         throw new TransportException("Coukd not create netty transport", future.getCause());
      }
   }

   @Override
   protected void writeBuffer(byte[] toAppend) {
      channel.write(toAppend);
   }

   @Override
   public void writeByte(short toWrite) {
      lastWrite = channel.write(toWrite);
   }

   @Override
   public void writeVInt(int length) {
      lastWrite = channel.write(length);
   }

   @Override
   public void writeVLong(long l) {
      lastWrite = channel.write(l);
   }


   @Override
   public void flush() {
      try {
         lastWrite.await();
      } catch (InterruptedException e) {
         throw new TransportException(e);
      }
   }

   @Override
   public long readVLong() {
      return decoder.readVLong();
   }

   @Override
   public int readVInt() {
      return decoder.readVInt();
   }

   @Override
   public short readByte() {
      return decoder.readByte();
   }

   @Override
   public void release() {
      // TODO: Customise this generated block
   }

   @Override
   protected void readBuffer(byte[] bufferToFill) {
      decoder.fillBuffer(bufferToFill);
   }
}
