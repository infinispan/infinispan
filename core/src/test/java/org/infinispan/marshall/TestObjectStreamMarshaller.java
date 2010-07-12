package org.infinispan.marshall;

import com.thoughtworks.xstream.XStream;
import org.infinispan.io.ByteBuffer;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.util.Util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * A dummy marshaller impl that uses object streams converted via XStream as current JBoss Marshalling implementation
 * requires that the objects being serialized/deserialized implement Serializable or Externalizable.
 *
 * @author Manik Surtani
 */
public class TestObjectStreamMarshaller extends AbstractStreamingMarshaller {
   XStream xs = new XStream();
   boolean debugXml = false;

   public TestObjectStreamMarshaller(boolean debugXml) {
      this.debugXml = debugXml;
   }

   public TestObjectStreamMarshaller() {
   }

   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant) throws IOException {
      return new ObjectOutputStream(os);
   }

   public void finishObjectOutput(ObjectOutput oo) {
      Util.flushAndCloseOutput(oo);
   }

   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      String xml = xs.toXML(obj);
      debug("Writing: \n" + xml);
      out.writeObject(xml);
   }

   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      String xml = (String) in.readObject();
      debug("Reading: \n" + xml);
      return xs.fromXML(xml);
   }

   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      return new ObjectInputStream(is);
   }

   public void finishObjectInput(ObjectInput oi) {
      if (oi != null) {
         try {
            oi.close();
         } catch (IOException e) {
         }
      }
   }

   public ByteBuffer objectToBuffer(Object o) throws IOException {
      byte[] b = objectToByteBuffer(o);
      return new ByteBuffer(b, 0, b.length);
   }

   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      byte[] newBytes = new byte[length];
      System.arraycopy(buf, offset, newBytes, 0, length);
      return objectFromByteBuffer(newBytes);
   }

   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException {
      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(estimatedSize);
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      objectToObjectStream(obj, oos);
      oos.flush();
      oos.close();
      baos.close();
      return baos.toByteArray();
   }

   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return objectFromObjectStream(new ObjectInputStream(new ByteArrayInputStream(buf)));
   }

   private void debug(String s) {
      if (debugXml) {
         System.out.println("TestObjectStreamMarshaller: " + s);
      }
   }
}
