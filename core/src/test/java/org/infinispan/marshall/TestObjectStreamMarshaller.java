package org.infinispan.marshall;

import com.thoughtworks.xstream.XStream;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.ExposedByteArrayOutputStream;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;

import java.io.*;

/**
 * A dummy marshaller impl that uses object streams converted via XStream as current JBoss Marshalling implementation
 * requires that the objects being serialized/deserialized implement Serializable or Externalizable.
 *
 * @author Manik Surtani
 */
public class TestObjectStreamMarshaller extends AbstractMarshaller implements StreamingMarshaller {
   XStream xs = new XStream();
   boolean debugXml = false;

   public TestObjectStreamMarshaller(boolean debugXml) {
      this.debugXml = debugXml;
   }

   public TestObjectStreamMarshaller() {
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int expectedByteSize) throws IOException {
      return new ObjectOutputStream(os);
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      Util.flushAndCloseOutput(oo);
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      String xml = xs.toXML(obj);
      debug("Writing: \n" + xml);
      out.writeObject(xml);
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      String xml = (String) in.readObject();
      debug("Reading: \n" + xml);
      return xs.fromXML(xml);
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      return new ObjectInputStream(is);
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      if (oi != null) {
         try {
            oi.close();
         } catch (IOException e) {
         }
      }
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException {
      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(estimatedSize);
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      objectToObjectStream(o, oos);
      oos.flush();
      oos.close();
      baos.close();
      byte[] b = baos.toByteArray();
      return new ByteBufferImpl(b, 0, b.length);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return objectFromObjectStream(new ObjectInputStream(new ByteArrayInputStream(buf, offset, length)));
   }

   @Override
   public boolean isMarshallable(Object o) {
      return (o instanceof Serializable || o instanceof Externalizable);
   }

   private void debug(String s) {
      if (debugXml) {
         System.out.println("TestObjectStreamMarshaller: " + s);
      }
   }

   @Override
   public void stop() {
      //No-op
   }

   @Override
   public void start() {
      //No-op
   }

}
