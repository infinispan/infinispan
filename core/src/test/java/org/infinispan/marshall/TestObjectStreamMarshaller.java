package org.infinispan.marshall;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
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
 * A dummy {@link PersistenceMarshaller} impl.
 * N.B.: When an instance of this class is no longer needed please invoke TestObjectStreamMarshaller.stop on it.
 *
 * @author Manik Surtani
 */
@Scope(Scopes.GLOBAL)
public class TestObjectStreamMarshaller implements PersistenceMarshaller {

   private static final Log log = LogFactory.getLog(TestObjectStreamMarshaller.class);

   private final PersistenceMarshallerImpl marshaller;

   public final EmbeddedCacheManager cacheManager;

   public TestObjectStreamMarshaller() {
      this(null);
   }

   public TestObjectStreamMarshaller(SerializationContextInitializer sci) {
      cacheManager = TestCacheManagerFactory.createCacheManager(sci, new ConfigurationBuilder());
      marshaller = (PersistenceMarshallerImpl) ComponentRegistry.of(cacheManager.getCache()).getPersistenceMarshaller();
   }

   public ClassAllowList getAllowList() {
      return cacheManager.getClassAllowList();
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
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      return marshaller.objectToBuffer(o);
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return marshaller.getBufferSizePredictor(o);
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
   public Marshaller getUserMarshaller() {
      return marshaller.getUserMarshaller();
   }

   @Override
   public int sizeEstimate(Object o) {
      return marshaller.sizeEstimate(o);
   }
}
