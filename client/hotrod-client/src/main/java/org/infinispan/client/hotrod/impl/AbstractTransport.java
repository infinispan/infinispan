package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.impl.transport.TransportException;

import java.io.IOException;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public abstract class AbstractTransport implements Transport {

   public byte[] readByteArray() {
         int responseLength = readVInt();
         byte[] bufferToFill = new byte[responseLength];
         readBuffer(bufferToFill);
         return bufferToFill;
   }

   @Override
   public String readString() {
      byte[] strContent = readByteArray();
      return new String(strContent);//todo take care of encoding here
   }

   protected abstract void readBuffer(byte[] bufferToFill);

   public void writeByteArray(byte[] toAppend) {
      writeVInt(toAppend.length);
      writeBuffer(toAppend);
   }

   protected abstract void writeBuffer(byte[] toAppend);
}
