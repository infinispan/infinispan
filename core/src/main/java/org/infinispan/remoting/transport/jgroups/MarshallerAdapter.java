package org.infinispan.remoting.transport.jgroups;

import org.infinispan.io.ByteBuffer;
import org.infinispan.marshall.StreamingMarshaller;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;

/**
 * Bridge between JGroups and Infinispan marshallers
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MarshallerAdapter implements RpcDispatcher.Marshaller2 {
   StreamingMarshaller m;

   public MarshallerAdapter(StreamingMarshaller m) {
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
