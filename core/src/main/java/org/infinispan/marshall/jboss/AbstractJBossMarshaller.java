package org.infinispan.marshall.jboss;

import org.infinispan.io.ByteBuffer;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.marshall.AbstractMarshaller;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.ConcurrentWeakKeyHashMap;
import org.infinispan.util.logging.BasicLogFactory;
import org.jboss.logging.BasicLogger;
import org.jboss.marshalling.ExceptionListener;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.TraceInformation;
import org.jboss.marshalling.Unmarshaller;
import org.jboss.marshalling.reflect.SunReflectiveCreator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.concurrent.ConcurrentMap;

import static org.infinispan.util.ReflectionUtil.EMPTY_CLASS_ARRAY;
import static org.infinispan.util.Util.EMPTY_OBJECT_ARRAY;

/**
 * Common parent for both embedded and standalone JBoss Marshalling-based marshallers.
 *
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 5.0
 */
public abstract class AbstractJBossMarshaller extends AbstractMarshaller implements StreamingMarshaller {

   protected static final BasicLogger log = BasicLogFactory.getLog(AbstractJBossMarshaller.class);
   protected static final boolean trace = log.isTraceEnabled();
   protected static final JBossMarshallerFactory factory = new JBossMarshallerFactory();
   protected static final int DEF_INSTANCE_COUNT = 16;
   protected static final int DEF_CLASS_COUNT = 8;
   private static final int PER_THREAD_REUSABLE_INSTANCES = 6;

   protected final MarshallingConfiguration baseCfg;

   /**
    * Marshaller thread local. In non-internal marshaller usages, such as Java
    * Hot Rod client, this is a singleton shared by all so no urgent need for
    * static here. JBMAR clears pretty much any state during finish(), so no
    * urgent need to clear the thread local since it shouldn't be leaking.
    * It might take a long time to warmup and pre-initialize all needed instances!
    */
   private final ThreadLocal<PerThreadInstanceHolder> marshallerTL = new ThreadLocal<PerThreadInstanceHolder>() {
      @Override
      protected PerThreadInstanceHolder initialValue() {
         MarshallingConfiguration cfg = baseCfg.clone();
         return new PerThreadInstanceHolder(cfg);
      }
   };

   /**
    * Cache of classes that are considered to be marshallable. Since checking
    * whether a type is marshallable requires attempting to marshalling them,
    * a cache for the types that are known to be marshallable or not is
    * advantageous.
    */
   private final ConcurrentMap<Class, Boolean> isMarshallableMap = new ConcurrentWeakKeyHashMap<Class, Boolean>();

   public AbstractJBossMarshaller() {
      // Class resolver now set when marshaller/unmarshaller will be created
      baseCfg = new MarshallingConfiguration();
      baseCfg.setExternalizerCreator(new SunReflectiveCreator());
      baseCfg.setExceptionListener(new DebuggingExceptionListener());
      baseCfg.setClassExternalizerFactory(new SerializeWithExtFactory());
      baseCfg.setInstanceCount(DEF_INSTANCE_COUNT);
      baseCfg.setClassCount(DEF_CLASS_COUNT);
      baseCfg.setVersion(3);
   }

   final public void objectToObjectStream(final Object obj, final ObjectOutput out) throws IOException {
      out.writeObject(obj);
   }

   @Override
   final protected ByteBuffer objectToBuffer(final Object o, final int estimatedSize) throws IOException {
      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(estimatedSize);
      ObjectOutput marshaller = startObjectOutput(baos, false, estimatedSize);
      try {
         objectToObjectStream(o, marshaller);
      } finally {
         finishObjectOutput(marshaller);
      }
      return new ByteBuffer(baos.getRawBuffer(), 0, baos.size());
   }

   final public ObjectOutput startObjectOutput(final OutputStream os, final boolean isReentrant, final int estimatedSize) throws IOException {
      PerThreadInstanceHolder instanceHolder = marshallerTL.get();
      org.jboss.marshalling.Marshaller marshaller = instanceHolder.getMarshaller(estimatedSize);
      marshaller.start(Marshalling.createByteOutput(os));
      return marshaller;
   }

   final public ObjectOutput startObjectOutput(final OutputStream os, final boolean isReentrant) throws IOException {
      return startObjectOutput(os, isReentrant, 512);
   }

   final public void finishObjectOutput(final ObjectOutput oo) {
      try {
         if (trace) log.trace("Stop marshaller");

         ((org.jboss.marshalling.Marshaller) oo).finish();
      } catch (IOException ignored) {
      }
   }

   @Override
   final public Object objectFromByteBuffer(final byte[] buf, final int offset, final int length) throws IOException,
           ClassNotFoundException {
      ByteArrayInputStream is = new ByteArrayInputStream(buf, offset, length);
      ObjectInput unmarshaller = startObjectInput(is, false);
      Object o = null;
      try {
         o = objectFromObjectStream(unmarshaller);
      } finally {
         finishObjectInput(unmarshaller);
      }
      return o;
   }

   final public ObjectInput startObjectInput(final InputStream is, final boolean isReentrant) throws IOException {
      PerThreadInstanceHolder instanceHolder = marshallerTL.get();
      Unmarshaller unmarshaller = instanceHolder.getUnmarshaller();

      if (trace)
         log.tracef("Start unmarshaller after retrieving marshaller from %s",
                   isReentrant ? "factory" : "thread local");

      unmarshaller.start(Marshalling.createByteInput(is));
      return unmarshaller;
   }

   final public Object objectFromObjectStream(final ObjectInput in) throws IOException, ClassNotFoundException {
      return in.readObject();
   }

   final public void finishObjectInput(final ObjectInput oi) {
      try {
         if (trace)
            log.trace("Stop unmarshaller");

         if (oi != null) ((Unmarshaller) oi).finish();
      } catch (IOException ignored) {
      }
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      Class clazz = o.getClass();
      Object isClassMarshallable = isMarshallableMap.get(clazz);
      if (isClassMarshallable != null) {
         return (Boolean) isClassMarshallable;
      } else {
         if (isMarshallableCandidate(o)) {
            boolean isMarshallable = true;
            try {
               objectToBuffer(o);
            } catch (Exception e) {
               isMarshallable = false;
               throw e;
            } finally {
               isMarshallableMap.putIfAbsent(clazz, isMarshallable);
            }
            return isMarshallable;
         }
         return false;
      }
   }

   public void stop() {
       // Clear class cache
      isMarshallableMap.clear();
   }

   protected boolean isMarshallableCandidate(Object o) {
      return o instanceof Serializable;
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
            Class returnType = urls.getClass();
            Class[] parameterTypes = EMPTY_CLASS_ARRAY;
            Method getURLs = cl.getClass().getMethod("getURLs", parameterTypes);
            if (returnType.isAssignableFrom(getURLs.getReturnType())) {
               Object[] args = EMPTY_OBJECT_ARRAY;
               urls = (URL[]) getURLs.invoke(cl, args);
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
         if (availableUnMarshallerIndex == PER_THREAD_REUSABLE_INSTANCES) {
            //we're above the pool threshold: make a throw-away-after usage Marshaller
            configuration.setBufferSize(512);//reset to default
            return factory.createUnmarshaller(configuration);
         }
         else {
            ExtendedRiverUnmarshaller unMarshaller = reusableUnMarshaller[availableUnMarshallerIndex];
            if (unMarshaller != null) {
               availableUnMarshallerIndex++;
               return unMarshaller;
            }
            else {
               configuration.setBufferSize(512);//reset to default
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
            configuration.setBufferSize(estimatedSize);
            return factory.createMarshaller(configuration);
         }
         else {
            ExtendedRiverMarshaller marshaller = reusableMarshaller[availableMarshallerIndex];
            if (marshaller != null) {
               availableMarshallerIndex++;
               return marshaller;
            }
            else {
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

}
