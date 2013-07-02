package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;

/**
 * Bridge between JGroups and Infinispan marshallers
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MarshallerAdapter implements RpcDispatcher.Marshaller {
   StreamingMarshaller m;

   public MarshallerAdapter(StreamingMarshaller m) {
      this.m = m;
   }

   @Override
   public Buffer objectToBuffer(Object obj) throws Exception {
      return toBuffer(m.objectToBuffer(obj));
   }

   @Override
   public Object objectFromBuffer(byte[] buf, int offset, int length) throws Exception {
      return m.objectFromByteBuffer(buf, offset, length);
   }

   private Buffer toBuffer(ByteBuffer bb) {
      return new Buffer(bb.getBuf(), bb.getOffset(), bb.getLength());
   }
}
