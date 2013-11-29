package org.infinispan.marshall;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * A dummy marshaller impl. Under the hood instantiates an {@link StreamingMarshaller}.
 * N.B.: When an instance of this class is no longer needed please invoke TestObjectStreamMarshaller.stop on it.
 *
 * @author Manik Surtani
 */
public class TestObjectStreamMarshaller extends AbstractMarshaller implements StreamingMarshaller {

   private static Log log = LogFactory.getLog(TestObjectStreamMarshaller.class);

   private final StreamingMarshaller marshaller;

   public final EmbeddedCacheManager cacheManager;

   public TestObjectStreamMarshaller() {
      cacheManager = TestCacheManagerFactory.createCacheManager();
      marshaller = cacheManager.getCache().getAdvancedCache().getComponentRegistry().getCacheMarshaller();
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int expectedByteSize) throws IOException {
      return marshaller.startObjectOutput(os, isReentrant, expectedByteSize);
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
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      return marshaller.objectFromObjectStream(in);
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
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException, InterruptedException {
      return marshaller.objectToBuffer(o);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return marshaller.objectFromByteBuffer(buf, offset, length);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return marshaller.isMarshallable(o);
   }

   @Override
   @Stop
   public void stop() {
      log.trace("TestObjectStreamMarshaller.stop()");
      TestingUtil.killCacheManagers(cacheManager);
   }

   @Override
   public void start() {
   }
}
