package org.infinispan.tools.store.migrator.marshaller;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Map;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * LegacyVersionAwareMarshaller that is used to read bytes marshalled using Infinispan 8.x. This is useful for providing
 * a migration path between 8.x and 9.x stores.
 */
public class LegacyVersionAwareMarshaller extends AbstractMarshaller implements StreamingMarshaller {
   private static final Log log = LogFactory.getLog(LegacyVersionAwareMarshaller.class);
   private final LegacyJBossMarshaller defaultMarshaller;

   public LegacyVersionAwareMarshaller(Map<Integer, ? extends AdvancedExternalizer<?>> externalizerMap) {
      this.defaultMarshaller = new LegacyJBossMarshaller(this, externalizerMap);
   }

   @Override
   public void stop() {
   }

   @Override
   public void start() {
   }

   @Override
   protected ByteBuffer objectToBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object objectFromByteBuffer(byte[] bytes, int offset, int len) throws IOException, ClassNotFoundException {
      ByteArrayInputStream is = new ByteArrayInputStream(bytes, offset, len);
      ObjectInput in = startObjectInput(is, false);
      Object o = null;
      try {
         o = defaultMarshaller.objectFromObjectStream(in);
      } finally {
         finishObjectInput(in);
      }
      return o;
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      ObjectInput in = defaultMarshaller.startObjectInput(is, isReentrant);
      try {
         in.readShort();
      }
      catch (Exception e) {
         finishObjectInput(in);
         log.unableToReadVersionId();
         throw new IOException("Unable to read version id from first two bytes of stream: " + e.getMessage());
      }
      return in;
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      defaultMarshaller.finishObjectInput(oi);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return defaultMarshaller.isMarshallable(o);
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      throw new UnsupportedOperationException();
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, final int estimatedSize) throws IOException {
      throw new UnsupportedOperationException();
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      throw new UnsupportedOperationException();
   }
}
