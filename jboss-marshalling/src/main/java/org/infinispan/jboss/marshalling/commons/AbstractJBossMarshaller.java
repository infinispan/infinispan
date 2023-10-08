package org.infinispan.jboss.marshalling.commons;

import static org.infinispan.commons.util.ReflectionUtil.EMPTY_CLASS_ARRAY;
import static org.infinispan.commons.util.Util.EMPTY_OBJECT_ARRAY;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URL;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.LazyByteArrayOutputStream;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.jboss.marshalling.ExceptionListener;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.TraceInformation;
import org.jboss.marshalling.Unmarshaller;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Common parent for both embedded and standalone JBoss Marshalling-based marshallers.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @author Dan Berindei
 * @since 5.0
 */
public abstract class AbstractJBossMarshaller extends AbstractMarshaller implements StreamingMarshaller {

   protected static final Log log = LogFactory.getLog(AbstractJBossMarshaller.class);
   protected static final JBossMarshallerFactory factory = new JBossMarshallerFactory();
   protected static final int DEF_INSTANCE_COUNT = 16;
   protected static final int DEF_CLASS_COUNT = 8;
   private static final int PER_THREAD_REUSABLE_INSTANCES = 6;
   private static final int RIVER_INTERNAL_BUFFER = 512;

   protected final MarshallingConfiguration baseCfg;

   /**
    * Marshaller thread local. In non-internal marshaller usages, such as Java
    * Hot Rod client, this is a singleton shared by all so no urgent need for
    * static here. JBMAR clears pretty much any state during finish(), so no
    * urgent need to clear the thread local since it shouldn't be leaking.
    * It might take a long time to warmup and pre-initialize all needed instances!
    */
   private final Cache<Thread, PerThreadInstanceHolder> marshallerTL = Caffeine.newBuilder().weakKeys().build();

   public AbstractJBossMarshaller() {
      // Class resolver now set when marshaller/unmarshaller will be created
      baseCfg = new MarshallingConfiguration();
      baseCfg.setExceptionListener(new DebuggingExceptionListener());
      baseCfg.setInstanceCount(DEF_INSTANCE_COUNT);
      baseCfg.setClassCount(DEF_CLASS_COUNT);
      baseCfg.setVersion(3);
   }

   @Override
   final public void objectToObjectStream(final Object obj, final ObjectOutput out) throws IOException {
      out.writeObject(obj);
   }

   @Override
   final protected ByteBuffer objectToBuffer(final Object o, final int estimatedSize) throws IOException {
      LazyByteArrayOutputStream baos = new LazyByteArrayOutputStream(estimatedSize);
      ObjectOutput marshaller = startObjectOutput(baos, false, estimatedSize);
      try {
         objectToObjectStream(o, marshaller);
      } finally {
         finishObjectOutput(marshaller);
      }
      return ByteBufferImpl.create(baos.getRawBuffer(), 0, baos.size());
   }

   @Override
   final public ObjectOutput startObjectOutput(final OutputStream os, final boolean isReentrant, final int estimatedSize) throws IOException {
      PerThreadInstanceHolder instanceHolder = getPerThreadInstanceHolder();
      org.jboss.marshalling.Marshaller marshaller = instanceHolder.getMarshaller(estimatedSize);
      marshaller.start(Marshalling.createByteOutput(os));
      return marshaller;
   }

   @Override
   final public void finishObjectOutput(final ObjectOutput oo) {
      try {
         if (log.isTraceEnabled()) log.trace("Stop marshaller");

         ((org.jboss.marshalling.Marshaller) oo).finish();
      } catch (IOException ignored) {
      }
   }

   @Override
   final public Object objectFromByteBuffer(final byte[] buf, final int offset, final int length) throws IOException,
         ClassNotFoundException {
      ByteArrayInputStream is = new ByteArrayInputStream(buf, offset, length);
      ObjectInput unmarshaller = startObjectInput(is, false);
      Object o;
      try {
         o = objectFromObjectStream(unmarshaller);
      } finally {
         finishObjectInput(unmarshaller);
      }
      return o;
   }

   @Override
   final public ObjectInput startObjectInput(final InputStream is, final boolean isReentrant) throws IOException {
      PerThreadInstanceHolder instanceHolder = getPerThreadInstanceHolder();
      Unmarshaller unmarshaller = instanceHolder.getUnmarshaller();

      if (log.isTraceEnabled())
         log.tracef("Start unmarshaller after retrieving marshaller from %s",
               isReentrant ? "factory" : "thread local");

      unmarshaller.start(Marshalling.createByteInput(is));
      return unmarshaller;
   }

   @Override
   final public Object objectFromObjectStream(final ObjectInput in) throws IOException, ClassNotFoundException {
      return in.readObject();
   }

   @Override
   final public void finishObjectInput(final ObjectInput oi) {
      try {
         if (log.isTraceEnabled())
            log.trace("Stop unmarshaller");

         if (oi != null) ((Unmarshaller) oi).finish();
      } catch (IOException ignored) {
      }
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      Class<?> clazz = o.getClass();
      boolean containsMarshallable = marshallableTypeHints.isKnownMarshallable(clazz);
      if (containsMarshallable) {
         boolean marshallable = marshallableTypeHints.isMarshallable(clazz);
         if (log.isTraceEnabled())
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
            return true;
         }
         return false;
      }
   }

   @Override
   public void start() {
      // No-op
   }

   @Override
   public void stop() {
      // Clear class cache
      marshallableTypeHints.clear();
      marshallerTL.invalidateAll();
   }

   protected boolean isMarshallableCandidate(Object o) {
      return o instanceof Serializable;
   }

   private PerThreadInstanceHolder getPerThreadInstanceHolder() {
      final Thread thread = Thread.currentThread();
      PerThreadInstanceHolder holder = marshallerTL.getIfPresent(thread);
      if (holder == null) {
         holder = new PerThreadInstanceHolder(baseCfg.clone());
         marshallerTL.put(thread, holder);
      }
      return holder;
   }

   protected static final class DebuggingExceptionListener implements ExceptionListener {
      private static final URL[] EMPTY_URLS = {};

      @Override
      public void handleMarshallingException(final Throwable problem, final Object subject) {
         if (log.isDebugEnabled()) {
            TraceInformation.addUserInformation(problem, "toString = " + subject.toString());
         }
      }

      @Override
      public void handleUnmarshallingException(final Throwable problem, final Class<?> subjectClass) {
         if (log.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            ClassLoader cl = subjectClass.getClassLoader();
            builder.append("classloader hierarchy:");
            ClassLoader parent = cl;
            while (parent != null) {
               if (parent.equals(cl)) {
                  builder.append("\n\t\t-> type classloader = ").append(parent);
               } else {
                  builder.append("\n\t\t-> parent classloader = ").append(parent);
               }
               URL[] urls = getClassLoaderURLs(parent);

               if (urls != null) {
                  for (URL u : urls) builder.append("\n\t\t->...").append(u);
               }

               parent = parent.getParent();
            }
            TraceInformation.addUserInformation(problem, builder.toString());
         }
      }

      @Override
      public void handleUnmarshallingException(Throwable problem) {
         // no-op
      }

      private static URL[] getClassLoaderURLs(final ClassLoader cl) {
         URL[] urls = EMPTY_URLS;
         try {
            Class<?> returnType = urls.getClass();
            Method getURLs = cl.getClass().getMethod("getURLs", EMPTY_CLASS_ARRAY);
            if (returnType.isAssignableFrom(getURLs.getReturnType())) {
               urls = (URL[]) getURLs.invoke(cl, EMPTY_OBJECT_ARRAY);
            }
         } catch (Exception ignore) {
         }
         return urls;
      }

   }

   private static final class PerThreadInstanceHolder implements RiverCloseListener {

      final MarshallingConfiguration configuration;
      final ExtendedRiverMarshaller[] reusableMarshaller = new ExtendedRiverMarshaller[PER_THREAD_REUSABLE_INSTANCES];
      int availableMarshallerIndex = 0;
      final ExtendedRiverUnmarshaller[] reusableUnMarshaller = new ExtendedRiverUnmarshaller[PER_THREAD_REUSABLE_INSTANCES];
      int availableUnMarshallerIndex = 0;

      PerThreadInstanceHolder(final MarshallingConfiguration threadDedicatedConfiguration) {
         this.configuration = threadDedicatedConfiguration;
      }

      Unmarshaller getUnmarshaller() throws IOException {
         //as opposing to getMarshaller(int), in this case we don't have a good hint about initial buffer sizing
         if (availableUnMarshallerIndex == PER_THREAD_REUSABLE_INSTANCES) {
            //we're above the pool threshold: make a throw-away-after usage Marshaller
            configuration.setBufferSize(512);//reset to default as it might be changed by getMarshaller
            return factory.createUnmarshaller(configuration);
         } else {
            ExtendedRiverUnmarshaller unMarshaller = reusableUnMarshaller[availableUnMarshallerIndex];
            if (unMarshaller != null) {
               availableUnMarshallerIndex++;
               return unMarshaller;
            } else {
               configuration.setBufferSize(RIVER_INTERNAL_BUFFER);//reset to default as it might be changed by getMarshaller
               unMarshaller = factory.createUnmarshaller(configuration);
               unMarshaller.setCloseListener(this);
               reusableUnMarshaller[availableUnMarshallerIndex] = unMarshaller;
               availableUnMarshallerIndex++;
               return unMarshaller;
            }
         }
      }

      ExtendedRiverMarshaller getMarshaller(int estimatedSize) throws IOException {
         if (availableMarshallerIndex == PER_THREAD_REUSABLE_INSTANCES) {
            //we're above the pool threshold: make a throw-away-after usage Marshaller
            //setting the buffer as cheap as possible:
            configuration.setBufferSize(estimatedSize);
            return factory.createMarshaller(configuration);
         } else {
            ExtendedRiverMarshaller marshaller = reusableMarshaller[availableMarshallerIndex];
            if (marshaller != null) {
               availableMarshallerIndex++;
               return marshaller;
            } else {
               //we're going to pool this one, make sure the buffer size is set to a reasonable value
               //as we might have changed it previously:
               configuration.setBufferSize(RIVER_INTERNAL_BUFFER);
               marshaller = factory.createMarshaller(configuration);
               marshaller.setCloseListener(this);
               reusableMarshaller[availableMarshallerIndex] = marshaller;
               availableMarshallerIndex++;
               return marshaller;
            }
         }
      }

      @Override
      public void closeMarshaller() {
         availableMarshallerIndex--;
      }

      @Override
      public void closeUnmarshaller() {
         availableUnMarshallerIndex--;
      }
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_JBOSS_MARSHALLING;
   }
}
