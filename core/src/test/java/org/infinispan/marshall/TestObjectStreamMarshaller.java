package org.infinispan.marshall;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.persistence.impl.PersistenceMarshallerImpl;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A dummy marshaller impl. Under the hood instantiates an {@link StreamingMarshaller}.
 * N.B.: When an instance of this class is no longer needed please invoke TestObjectStreamMarshaller.stop on it.
 *
 * @author Manik Surtani
 */
@Scope(Scopes.GLOBAL)
public class TestObjectStreamMarshaller extends AbstractMarshaller implements PersistenceMarshaller {

   private static Log log = LogFactory.getLog(TestObjectStreamMarshaller.class);

   private final PersistenceMarshallerImpl marshaller;

   public final EmbeddedCacheManager cacheManager;

   public TestObjectStreamMarshaller() {
      cacheManager = TestCacheManagerFactory.createCacheManager();
      marshaller = (PersistenceMarshallerImpl) cacheManager.getCache().getAdvancedCache().getComponentRegistry().getPersistenceMarshaller();
   }

   public ClassWhiteList getWhiteList() {
      return cacheManager.getClassWhiteList();
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
   public void writeObject(Object o, OutputStream out) throws IOException {
      marshaller.writeObject(o, out);
   }

   @Override
   public Object readObject(InputStream in) throws ClassNotFoundException, IOException {
      return marshaller.readObject(in);
   }

   @Override
   public boolean isMarshallable(Object o) {
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

   @Override
   public MediaType mediaType() {
      return marshaller.mediaType();
   }

   @Override
   public void register(SerializationContextInitializer initializer) {
      marshaller.register(initializer);
   }

   @Override
   public int sizeEstimate(Object o) {
      return marshaller.sizeEstimate(o);
   }
}
