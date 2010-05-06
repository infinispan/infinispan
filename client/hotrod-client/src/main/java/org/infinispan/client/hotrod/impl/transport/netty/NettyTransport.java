package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.transport.AbstractTransport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
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

   private static Log log = LogFactory.getLog(NettyTransport.class);

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
         throw new TransportException("Could not create netty transport", future.getCause());
      }
   }

   @Override
   protected void writeBytes(byte[] toAppend) {
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
      log.trace("About to close the channel: " + channel);
      channel.close();
   }

   @Override
   public byte[] readByteArray(int size) {
      byte[] bytes = new byte[size];
      decoder.fillBuffer(bytes);
      return bytes;
   }
}
