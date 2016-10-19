package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.function.BiConsumer;

import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ExposedByteArrayOutputStream;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.MarshallableTypeHints;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.commons.marshall.SerializeFunctionWith;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.ClassToExternalizerMap.IdToExternalizerMap;
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
   private static final boolean trace = log.isTraceEnabled();

   static final int ID_NULL                        = 0x00;
   static final int ID_PRIMITIVE                   = 0x01;
   static final int ID_INTERNAL                    = 0x02;
   static final int ID_EXTERNAL                    = 0x03;
   static final int ID_ANNOTATED                   = 0x04;
   static final int ID_UNKNOWN                     = 0x05;
   static final int NOT_FOUND                      = -1;

   private final MarshallableTypeHints marshallableTypeHints = new MarshallableTypeHints();

   private GlobalComponentRegistry gcr;
   private RemoteCommandsFactory cmdFactory;

   private ClassToExternalizerMap internalExts;
   private IdToExternalizerMap reverseInternalExts;
   private ClassToExternalizerMap externalExts;
   private IdToExternalizerMap reverseExternalExts;

   private Marshaller external;

   public GlobalMarshaller() {
   }

   public GlobalMarshaller(Marshaller external) {
      this.external = external;
   }

   @Inject
   public void inject(GlobalComponentRegistry gcr, RemoteCommandsFactory cmdFactory) {
      this.gcr = gcr;
      this.cmdFactory = cmdFactory;
   }

   @Override
   @Start(priority = 8) // Should start after the externalizer table and before transport
   public void start() {
      internalExts = InternalExternalizers.load(this, gcr, cmdFactory);
      reverseInternalExts = internalExts.reverseMap(128);
      if (trace) {
         log.tracef("Internal class to externalizer ids: %s", internalExts);
         log.tracef("Internal reverse externalizers: %s", reverseInternalExts);
      }

      externalExts = ExternalExternalizers.load(gcr.getGlobalConfiguration());
      reverseExternalExts = externalExts.reverseMap();
      if (trace) {
         log.tracef("External class to externalizer ids: %s", externalExts);
         log.tracef("External reverse externalizers: %s", reverseExternalExts);
      }

      if (external == null) {
         this.external = startDefaultExternalMarshaller(gcr.getGlobalConfiguration());
      }
   }

   public Marshaller startDefaultExternalMarshaller(GlobalConfiguration globalCfg) {
      StreamingMarshaller marshaller = new ExternalJBossMarshaller(this, globalCfg);
      marshaller.start();
      return marshaller;
   }

   @Override
   @Stop(priority = 11) // Stop after transport to avoid send/receive and marshaller not being ready
   public void stop() {
      internalExts.clear();
      reverseInternalExts.clear();
      externalExts.clear();
      reverseExternalExts.clear();
      stopDefaultExternalMarshaller();
   }

   public void stopDefaultExternalMarshaller() {
      if (external instanceof StreamingMarshaller)
         ((StreamingMarshaller) external).stop();
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      try {
         BytesObjectOutput out = writeObjectOutput(obj);
         return out.toBytes(); // trim out unused bytes
      } catch (java.io.NotSerializableException nse) {
         if (log.isDebugEnabled()) log.debug("Object is not serializable", nse);
         throw new NotSerializableException(nse.getMessage(), nse.getCause());
      }
   }

   private BytesObjectOutput writeObjectOutput(Object obj) throws IOException {
      BufferSizePredictor sizePredictor = marshallableTypeHints.getBufferSizePredictor(obj);
      BytesObjectOutput out = writeObjectOutput(obj, sizePredictor.nextSize(obj));
      sizePredictor.recordSize(out.pos);
      return out;
   }

   private BytesObjectOutput writeObjectOutput(Object obj, int estimatedSize) throws IOException {
      BytesObjectOutput out = new BytesObjectOutput(estimatedSize, this);
      writeNullableObject(obj, out);
      return out;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      BytesObjectInput in = BytesObjectInput.from(buf, this);
      return objectFromObjectInput(in);
   }

   private Object objectFromObjectInput(BytesObjectInput in) throws IOException, ClassNotFoundException {
      return readNullableObject(in);
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int estimatedSize) throws IOException {
      BytesObjectOutput out = new BytesObjectOutput(estimatedSize, this);
      return new StreamBytesObjectOutput(os, out);
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      out.writeObject(obj);
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      try {
         oo.flush();
      } catch (IOException e) {
         // ignored
      }
   }

   @Override
   public Object objectFromByteBuffer(byte[] bytes, int offset, int len) throws IOException, ClassNotFoundException {
      // Ignore length since boundary checks are not so useful here where the
      // unmarshalling code knows what to expect specifically. E.g. if reading
      // a byte[] subset within it, it's always appended with length.
      BytesObjectInput in = BytesObjectInput.from(bytes, offset, this);
      return objectFromObjectInput(in);
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
      // This is a very limited use case, e.g. reading from a JDBC ResultSet InputStream
      // So, this copying of the stream into a byte[] has not been problematic so far,
      // though it's not really ideal.
      int len = is.available();
      ExposedByteArrayOutputStream bytes;
      byte[] buf;
      if(len > 0) {
         bytes = new ExposedByteArrayOutputStream(len);
         buf = new byte[Math.min(len, 1024)];
      } else {
         // Some input stream providers do not implement available()
         bytes = new ExposedByteArrayOutputStream();
         buf = new byte[1024];
      }
      int bytesRead;
      while ((bytesRead = is.read(buf, 0, buf.length)) != -1) bytes.write(buf, 0, bytesRead);
      return objectFromByteBuffer(bytes.getRawBuffer(), 0, bytes.size());
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      Class<?> clazz = o.getClass();
      boolean containsMarshallable = marshallableTypeHints.isKnownMarshallable(clazz);
      if (containsMarshallable) {
         boolean marshallable = marshallableTypeHints.isMarshallable(clazz);
         if (trace)
            log.tracef("Marshallable type '%s' known and is marshallable=%b",
                  clazz.getName(), marshallable);

         return marshallable;
      } else {
         if (isMarshallableCandidate(o)) {
            boolean isMarshallable = true;
            try {
               objectToBuffer(o);
            } catch (Exception e) {
               isMarshallable = false;
               throw e;
            } finally {
               marshallableTypeHints.markMarshallable(clazz, isMarshallable);
            }
            return isMarshallable;
         }
         return false;
      }
   }

   private boolean isMarshallableCandidate(Object o) {
      return o instanceof Serializable
            || internalExts.get(o.getClass()) != null
            || externalExts.get(o.getClass()) != null
            || o.getClass().getAnnotation(SerializeWith.class) != null
            || isExternalMarshallable(o);
   }

   private boolean isExternalMarshallable(Object o) {
      try {
         return external.isMarshallable(o);
      } catch (Exception e) {
         throw new NotSerializableException(
               "Object of type " + o.getClass() + " expected to be marshallable", e);
      }
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return marshallableTypeHints.getBufferSizePredictor(o.getClass());
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      try {
         BytesObjectOutput out = writeObjectOutput(o);
         return out.toByteBuffer();
      } catch (java.io.NotSerializableException nse) {
         if (log.isDebugEnabled()) log.debug("Object is not serializable", nse);
         throw new NotSerializableException(nse.getMessage(), nse.getCause());
      }
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      try {
         BytesObjectOutput out = writeObjectOutput(obj, estimatedSize);
         return out.toBytes();
      } catch (java.io.NotSerializableException nse) {
         if (log.isDebugEnabled()) log.debug("Object is not serializable", nse);
         throw new NotSerializableException(nse.getMessage(), nse.getCause());
      }
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) {
      throw new UnsupportedOperationException("No longer in use");
   }

   public <T> Externalizer<T> findExternalizerFor(Object obj) {
      Class<?> clazz = obj.getClass();
      Externalizer ext = internalExts.get(clazz);
      if (ext == null) {
         ext = externalExts.get(clazz);
         if (ext == null)
            ext = findAnnotatedExternalizer(clazz);
      }

      return ext;
   }

   void writeNullableObject(Object obj, BytesObjectOutput out) throws IOException {
      if (obj == null)
         out.writeByte(ID_NULL);
      else
         writeNonNullableObject(obj, out);
   }


   Object readNullableObject(BytesObjectInput in) throws IOException, ClassNotFoundException {
      int type = in.readUnsignedByte();
      return type == ID_NULL ? null : readNonNullableObject(type, in);
   }

   BiConsumer<ObjectOutput, Object> findWriter(Object obj) {
      Class<?> clazz = obj.getClass();
      AdvancedExternalizer internalExt = internalExts.get(clazz);
      if (internalExt != null)
         return (out, object) -> {
            writeInternalClean(object, internalExt, out);
         };


      AdvancedExternalizer externalExt = externalExts.get(clazz);
      if (externalExt != null)
         return (out, object) -> writeExternalClean(object, externalExt, out);

      return null;
   }

   <T> AdvancedExternalizer<T> findExternalizerIn(ObjectInput in) throws IOException {
      int type = in.readUnsignedByte();
      switch (type) {
         case ID_INTERNAL:
            return reverseInternalExts.get(in.readUnsignedByte());
         case ID_EXTERNAL:
            return reverseExternalExts.get(in.readInt());
         default:
            return null;
      }
   }

   private void writeNonNullableObject(Object obj, BytesObjectOutput out) throws IOException {
      Class<?> clazz = obj.getClass();
      int id = Primitives.PRIMITIVES.get(clazz, NOT_FOUND);
      if (id != NOT_FOUND) {
         writePrimitive(obj, out, id);
      } else {
         AdvancedExternalizer ext = internalExts.get(clazz);
         if (ext != null) {
            writeInternal(obj, ext, out);
         } else {
            ext = externalExts.get(clazz);
            if (ext != null) {
               writeExternal(obj, ext, out);
            } else {
               Externalizer annotExt = findAnnotatedExternalizer(clazz);
               if (annotExt != null)
                  writeAnnotated(obj, out, annotExt);
               else
                  writeUnknown(obj, out);
            }
         }
      }
   }

   private void writeUnknown(Object obj, BytesObjectOutput out) throws IOException {
      out.writeByte(ID_UNKNOWN);
      if (external instanceof StreamingMarshaller)
         ((StreamingMarshaller) external).objectToObjectStream(obj, out);
      else {
         try {
            byte[] bytes = external.objectToByteBuffer(obj);
            out.writeInt(bytes.length);
            out.write(bytes);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
   }

   private void writeAnnotated(Object obj, BytesObjectOutput out, Externalizer ext) throws IOException {
      out.writeByte(ID_ANNOTATED);
      out.writeObject(ext.getClass());
      ext.writeObject(out, obj);
   }

   private void writeInternal(Object obj, AdvancedExternalizer ext, ObjectOutput out) throws IOException {
      out.writeByte(ID_INTERNAL);
      out.writeByte(ext.getId());
      ext.writeObject(out, obj);
   }

   private void writeInternalClean(Object obj, AdvancedExternalizer ext, ObjectOutput out) {
      try {
         writeInternal(obj, ext, out);
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }

   private void writeExternal(Object obj, AdvancedExternalizer ext, ObjectOutput out) throws IOException {
      out.writeByte(ID_EXTERNAL);
      out.writeInt(ext.getId());
      ext.writeObject(out, obj);
   }

   private void writeExternalClean(Object obj, AdvancedExternalizer ext, ObjectOutput out) {
      try {
         writeExternal(obj, ext, out);
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }

   private void writePrimitive(Object obj, BytesObjectOutput out, int id) throws IOException {
      out.writeByte(ID_PRIMITIVE);
      Primitives.writePrimitive(obj, out, id);
   }

   private <T> Externalizer<T> findAnnotatedExternalizer(Class<?> clazz) {
      try {
         SerializeWith serialAnn = clazz.getAnnotation(SerializeWith.class);
         if (serialAnn != null) {
            return (Externalizer<T>) serialAnn.value().newInstance();
         } else {
            SerializeFunctionWith funcSerialAnn = clazz.getAnnotation(SerializeFunctionWith.class);
            if (funcSerialAnn != null)
               return (Externalizer<T>) funcSerialAnn.value().newInstance();
         }

         return null;
      } catch (Exception e) {
         throw new IllegalArgumentException(String.format(
               "Cannot instantiate externalizer for %s", clazz), e);
      }
   }

   private Object readNonNullableObject(int type, BytesObjectInput in) throws IOException, ClassNotFoundException {
      switch (type) {
         case ID_PRIMITIVE:
            return Primitives.readPrimitive(in);
         case ID_INTERNAL:
            return readWithExternalizer(in.readUnsignedByte(), reverseInternalExts, in);
         case ID_EXTERNAL:
            return readWithExternalizer(in.readInt(), reverseExternalExts, in);
         case ID_ANNOTATED:
            return readAnnotated(in);
         case ID_UNKNOWN:
            return readUnknown(in);
         default:
            throw new IOException("Unknown type: " + type);
      }
   }

   private Object readWithExternalizer(int id, IdToExternalizerMap reverseMap, BytesObjectInput in)
         throws IOException, ClassNotFoundException {
      AdvancedExternalizer ext = reverseMap.get(id);
      return ext.readObject(in);
   }

   private Object readAnnotated(BytesObjectInput in) throws IOException, ClassNotFoundException {
      Class<? extends Externalizer> clazz =
            (Class<? extends Externalizer>) in.readObject();
      try {
         Externalizer ext = clazz.newInstance();
         return ext.readObject(in);
      } catch (Exception e) {
         throw new CacheException("Error instantiating class: " + clazz, e);
      }
   }


   private Object readUnknown(BytesObjectInput in) throws IOException, ClassNotFoundException {
      if (external instanceof StreamingMarshaller) {
         try {
            return ((StreamingMarshaller) external).objectFromObjectStream(in);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
         }
      } else {
         int length = in.readInt();
         byte[] bytes = new byte[length];
         in.readFully(bytes);
         return external.objectFromByteBuffer(bytes);
      }
   }

}
