package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.io.InputStream;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class InputStreamAdapter extends InputStream {

   private final ChannelBuffer buffer;

   public InputStreamAdapter(ChannelBuffer buffer) {
      this.buffer = buffer;
   }

   @Override
   public int read() throws IOException {
      synchronized (buffer) {
         if (buffer.readableBytes() == 0) {
            try {
               buffer.wait();
            } catch (InterruptedException e) {
               throw new TransportException(e);
            }
         }
         return buffer.readUnsignedByte();
      }
   }
}
