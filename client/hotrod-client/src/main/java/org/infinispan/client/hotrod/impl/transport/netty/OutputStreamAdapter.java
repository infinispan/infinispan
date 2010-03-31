package org.infinispan.client.hotrod.impl.transport.netty;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class OutputStreamAdapter extends OutputStream {

   ChannelBuffer buffer;


   public void setBuffer(ChannelBuffer buffer) {
      this.buffer = buffer;
   }

   @Override
   public void write(int b) throws IOException {
      buffer.writeByte(b);
   }
}
