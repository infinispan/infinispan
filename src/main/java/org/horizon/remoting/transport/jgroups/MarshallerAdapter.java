package org.horizon.remoting.transport.jgroups;

import org.horizon.io.ByteBuffer;
import org.horizon.marshall.Marshaller;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;

/**
 * Bridge between JGroups and Horizon marshallers
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class MarshallerAdapter implements RpcDispatcher.Marshaller2 {
   Marshaller m;

   public MarshallerAdapter(Marshaller m) {
      this.m = m;
   }

   public Buffer objectToBuffer(Object obj) throws Exception {
      return toBuffer(m.objectToBuffer(obj));
   }

   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws Exception {
      return m.objectFromByteBuffer(buf, offset, length);
   }

   public byte[] objectToByteBuffer(Object obj) throws Exception {
      return m.objectToByteBuffer(obj);
   }

   public Object objectFromByteBuffer(byte[] buf) throws Exception {
      return m.objectFromByteBuffer(buf);
   }

   private Buffer toBuffer(ByteBuffer bb) {
      return new Buffer(bb.getBuf(), bb.getOffset(), bb.getLength());
   }
}
