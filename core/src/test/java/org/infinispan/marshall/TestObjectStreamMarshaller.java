package org.infinispan.marshall;

import com.thoughtworks.xstream.XStream;
import org.infinispan.io.ByteBuffer;
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
 * A dummy marshaller impl that uses JBoss Marshalling object streams as they do not require that the objects being
 * serialized/deserialized implement Serializable.
 *
 * @author Manik Surtani
 */
public class TestObjectStreamMarshaller extends AbstractMarshaller {
   XStream xs = new XStream();
   boolean debugXml = false;

   public TestObjectStreamMarshaller(boolean debugXml) {
      this.debugXml = debugXml;
   }

   public TestObjectStreamMarshaller() {
   }

   public ObjectOutput startObjectOutput(OutputStream os) throws IOException {
      return new ObjectOutputStream(os);
   }

   public void finishObjectOutput(ObjectOutput oo) {
      Util.flushAndCloseOutput(oo);
   }

   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      String xml = xs.toXML(obj);
      debug("Writing: \n" + xml);
      out.writeUTF(xml);
   }

   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      String xml = in.readUTF();
      debug("Reading: \n" + xml);
      return xs.fromXML(xml);
   }

   public ObjectInput startObjectInput(InputStream is) throws IOException {
      return new ObjectInputStream(is);
   }

   public void finishObjectInput(ObjectInput oi) {
      Util.closeInput(oi);
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

   public byte[] objectToByteBuffer(Object obj) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
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
