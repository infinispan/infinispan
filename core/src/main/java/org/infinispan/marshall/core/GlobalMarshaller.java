package org.infinispan.marshall.core;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.ExposedByteArrayOutputStream;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.internal.InternalMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A globally-scoped marshaller. This is needed so that the transport layer
 * can unmarshall requests even before it's known which cache's marshaller can
 * do the job.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Scope(Scopes.GLOBAL)
public class GlobalMarshaller implements StreamingMarshaller {

   private static final Log log = LogFactory.getLog(GlobalMarshaller.class);
   private final boolean trace = log.isTraceEnabled();

   GlobalComponentRegistry gcr;
   RemoteCommandsFactory cmdFactory;

   InternalMarshaller internal;

//   private JBossMarshaller defaultMarshaller;
//
//   private ExternalizerTable extTable;
//   private GlobalConfiguration globalCfg;
//
//   @Inject
//   public void inject(ExternalizerTable extTable, GlobalConfiguration globalCfg) {
//      this.extTable = extTable;
//      this.globalCfg = globalCfg;
//   }

   @Inject
   public void inject(GlobalComponentRegistry gcr, RemoteCommandsFactory cmdFactory) {
      this.gcr = gcr;
      this.cmdFactory = cmdFactory;
   }

   @Override
   @Start(priority = 8) // Should start after the externalizer table and before transport
   public void start() {
      internal = new InternalMarshaller(gcr, cmdFactory);
      internal.start();
   }

   @Override
   @Stop(priority = 11) // Stop after transport to avoid send/receive and marshaller not being ready
   public void stop() {
      internal.stop();
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      try {
         return internal.objectToByteBuffer(obj);
      } catch (java.io.NotSerializableException nse) {
         if (log.isDebugEnabled()) log.debug("Object is not serializable", nse);
         throw new NotSerializableException(nse.getMessage(), nse.getCause());
      }
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return internal.objectFromByteBuffer(buf);
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int estimatedSize) throws IOException {
      return internal.startObjectOutput(os, isReentrant, estimatedSize);
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      internal.objectToObjectStream(obj, out);
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      internal.finishObjectOutput(oo);
   }

   @Override
   public Object objectFromByteBuffer(byte[] bytes, int offset, int len) throws IOException, ClassNotFoundException {
      return internal.objectFromByteBuffer(bytes, offset, len);
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
      return internal.objectFromInputStream(is);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return internal.isMarshallable(o);
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return internal.getBufferSizePredictor(o);
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      try {
         return internal.objectToBuffer(o);
      } catch (java.io.NotSerializableException nse) {
         if (log.isDebugEnabled()) log.debug("Object is not serializable", nse);
         throw new NotSerializableException(nse.getMessage(), nse.getCause());
      }
   }



//   @Override
//   protected ByteBuffer objectToBuffer(Object obj, int estimatedSize) throws IOException {
//      return internal.objectToBuffer(obj, estimatedSize);
//
////      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(estimatedSize);
////      ObjectOutput out = startObjectOutput(baos, false, estimatedSize);
////      try {
////         defaultMarshaller.objectToObjectStream(obj, out);
////      } catch (java.io.NotSerializableException nse) {
////         if (log.isDebugEnabled()) log.debug("Object is not serializable", nse);
////         throw new NotSerializableException(nse.getMessage(), nse.getCause());
////      } catch (IOException ioe) {
////         if (ioe.getCause() instanceof InterruptedException) {
////            if (trace) log.trace("Interrupted exception while marshalling", ioe.getCause());
////            throw (InterruptedException) ioe.getCause();
////         } else {
////            log.errorMarshallingObject(ioe, obj);
////            throw ioe;
////         }
////      } finally {
////         finishObjectOutput(out);
////      }
////      return new ByteBufferImpl(baos.getRawBuffer(), 0, baos.size());
//   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      return new byte[0];  // TODO: Customise this generated block
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      return null;
//      return defaultMarshaller.startObjectInput(is, isReentrant);
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
//      defaultMarshaller.finishObjectInput(oi);
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      return null;

//      /* No need to read version here. Clients should either be calling either:
//       * - startObjectInput() -> objectFromObjectStream() -> finishObjectInput()
//       * or
//       * - objectFromByteBuffer() // underneath it calls start/finish
//       * So, there's only need to read version during the start.
//       * First option is preferred when multiple objects are gonna be written.
//       */
//      try {
//         return defaultMarshaller.objectFromObjectStream(in);
//      } catch (EOFException e) {
//         IOException ee = new EOFException(
//               "The stream ended unexpectedly.  Please check whether the source of " +
//                     "the stream encountered any issues generating the stream.");
//         ee.initCause(e);
//         throw ee;
//      } catch (IOException ioe) {
//         if (trace) log.trace("Log exception reported", ioe);
//         if (ioe.getCause() instanceof InterruptedException)
//            throw (InterruptedException) ioe.getCause();
//         else
//            throw ioe;
//      }
   }

}
