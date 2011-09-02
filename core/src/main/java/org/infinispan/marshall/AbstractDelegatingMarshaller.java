package org.infinispan.marshall;

import org.infinispan.config.GlobalConfiguration;
import org.infinispan.io.ByteBuffer;
import org.infinispan.marshall.jboss.ExternalizerTable;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * With the introduction of global and cache marshallers, there's a need to
 * separate marshallers but still rely on the same marshalling backend as
 * previously. So, this class acts as a delegator for the new marshallers.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public abstract class AbstractDelegatingMarshaller implements StreamingMarshaller {

   // TODO: Should avoid hardcoding but it's really not likely to change
   protected VersionAwareMarshaller marshaller;
   private ExternalizerTable extTable;

   public void inject(ExternalizerTable extTable) {
      this.extTable = extTable;
   }

   public void start() {
      marshaller.start(extTable);
   }

   public void stop() {
      marshaller.stop();
   }

   protected VersionAwareMarshaller createMarshaller(GlobalConfiguration globalCfg, ClassLoader loader) {
      return (VersionAwareMarshaller) Util.getInstance(globalCfg.getMarshallerClass(), loader);
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant) throws IOException {
      return marshaller.startObjectOutput(os, isReentrant);
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      marshaller.finishObjectOutput(oo);
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      marshaller.objectToObjectStream(obj, out);
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      return marshaller.startObjectInput(is, isReentrant);
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      marshaller.finishObjectInput(oi);
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      return marshaller.objectFromObjectStream(in);
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
      return marshaller.objectFromInputStream(is);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      return marshaller.objectToByteBuffer(obj, estimatedSize);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      return marshaller.objectToByteBuffer(obj);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return marshaller.objectFromByteBuffer(buf);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return marshaller.objectFromByteBuffer(buf, offset, length);
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      return marshaller.objectToBuffer(o);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return marshaller.isMarshallable(o);
   }

   String getCacheName() {
      return marshaller.getCacheName();
   }

}
