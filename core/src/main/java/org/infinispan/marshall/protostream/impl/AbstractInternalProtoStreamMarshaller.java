package org.infinispan.marshall.protostream.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.MarshallableTypeHints;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.StreamAwareMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.RandomAccessOutputStream;
import org.infinispan.protostream.impl.RandomAccessOutputStreamImpl;
import org.infinispan.util.logging.Log;

/**
 * An abstract ProtoStream based {@link Marshaller} and {@link StreamAwareMarshaller} implementation that is the basis
 * of the Persistence and Global marshallers.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
@Scope(Scopes.GLOBAL)
public abstract class AbstractInternalProtoStreamMarshaller implements Marshaller, StreamAwareMarshaller {

   protected final MarshallableTypeHints marshallableTypeHints = new MarshallableTypeHints();

   @Inject protected SerializationContextRegistry ctxRegistry;
   @Inject @ComponentName(KnownComponentNames.USER_MARSHALLER)
   ComponentRef<Marshaller> userMarshallerRef;
   protected Marshaller userMarshaller;
   protected boolean skipUserMarshaller;
   protected Log log;

   abstract public ImmutableSerializationContext getSerializationContext();

   protected AbstractInternalProtoStreamMarshaller(Log log) {
      this.log = log;
   }

   @Start
   @Override
   public void start() {
      userMarshaller = userMarshallerRef.running();
   }

   public Marshaller getUserMarshaller() {
      return userMarshaller;
   }

   protected RandomAccessOutputStream objectToOutputStream(Object obj, int estimatedSize) {
      try {
         RandomAccessOutputStream os = new RandomAccessOutputStreamImpl(estimatedSize);
         ProtobufUtil.toWrappedStream(getSerializationContext(), os, obj);
         return os;
      } catch (Throwable t) {
         log.cannotMarshall(obj.getClass(), t);
         if (t instanceof MarshallingException)
            throw (MarshallingException) t;
         throw new MarshallingException(t.getMessage(), t.getCause());
      }
   }

   @Override
   public ByteBuffer objectToBuffer(Object obj) {
      if (obj == null)
         return ByteBufferImpl.EMPTY_INSTANCE;

      // Retrieve the size predictor without wrapping, as this will provide a more accurate estimate
      BufferSizePredictor sizePredictor = marshallableTypeHints.getBufferSizePredictor(obj);
      int estimatedSize = sizePredictor.nextSize(obj);
      if (requiresWrapping(obj)) {
         obj = new MarshallableUserObject<>(obj);
         // Add the additional bytes required by the object wrapper to the estimate
         estimatedSize = AbstractMarshallableWrapper.size(estimatedSize);
      }
      try (RandomAccessOutputStream os = objectToOutputStream(obj, estimatedSize)) {
         int length = os.getPosition();
         ByteBuffer buf = ByteBufferImpl.create(os.getByteBuffer());
         sizePredictor.recordSize(length);
         return buf;
      } catch (IOException e) {
         throw new MarshallingException(e);
      }
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) {
      if (obj == null)
         return Util.EMPTY_BYTE_ARRAY;

      if (requiresWrapping(obj)) {
         obj = new MarshallableUserObject<>(obj);
         // Add the additional bytes required by the object wrapper to the estimate
         estimatedSize = AbstractMarshallableWrapper.size(estimatedSize);
      }
      BufferSizePredictor sizePredictor = marshallableTypeHints.getBufferSizePredictor(obj);
      try (RandomAccessOutputStream os = objectToOutputStream(obj, estimatedSize)) {
         byte[] bytes = os.toByteArray();
         sizePredictor.recordSize(bytes.length);
         return bytes;
      } catch (IOException e) {
         throw new MarshallingException(e);
      }
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) {
      return objectToBuffer(obj).trim();
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException {
      return objectFromByteBuffer(buf, 0, buf.length);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException {
      return unwrapAndInit(ProtobufUtil.fromWrappedByteArray(getSerializationContext(), buf, offset, length));
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return marshallableTypeHints.getBufferSizePredictor(o.getClass());
   }

   @Override
   public void writeObject(Object o, OutputStream out) throws IOException {
      if (requiresWrapping(o))
         o = new MarshallableUserObject<>(o);
      ProtobufUtil.toWrappedStream(getSerializationContext(), out, o);
   }

   @Override
   public Object readObject(InputStream in) throws ClassNotFoundException, IOException {
      return unwrapAndInit(ProtobufUtil.fromWrappedStream(getSerializationContext(), in));
   }

   protected Object unwrapAndInit(Object o) {
      if (o instanceof MarshallableUserObject)
         return ((MarshallableUserObject<?>) o).get();

      return o;
   }

   @Override
   public boolean isMarshallable(Object o) {
      return isMarshallableWithProtoStream(o) || isUserMarshallable(o);
   }

   @Override
   public int sizeEstimate(Object o) {
      if (skipUserMarshaller)
         return marshallableTypeHints.getBufferSizePredictor(o.getClass()).nextSize(o);

      int userBytesEstimate = userMarshaller.getBufferSizePredictor(o.getClass()).nextSize(o);
      return MarshallableUserObject.size(userBytesEstimate);
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_PROTOSTREAM;
   }

   private boolean requiresWrapping(Object o) {
      return !skipUserMarshaller && !isMarshallableWithProtoStream(o);
   }

   protected boolean isMarshallableWithProtoStream(Object o) {
      return getSerializationContext().canMarshall(o.getClass());
   }

   private boolean isUserMarshallable(Object o) {
      try {
         return userMarshaller.isMarshallable(o);
      } catch (Exception ignore) {
         return false;
      }
   }
}
