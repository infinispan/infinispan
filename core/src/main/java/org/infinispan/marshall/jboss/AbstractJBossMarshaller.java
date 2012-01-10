package org.infinispan.marshall.jboss;

import org.infinispan.io.ByteBuffer;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.marshall.AbstractMarshaller;
import org.infinispan.util.ConcurrentWeakKeyHashMap;
import org.infinispan.util.logging.BasicLogFactory;
import org.jboss.logging.BasicLogger;
import org.jboss.marshalling.ExceptionListener;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.MarshallerFactory;
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
 * @since 5.0
 */
public abstract class AbstractJBossMarshaller extends AbstractMarshaller {

   protected static final BasicLogger log = BasicLogFactory.getLog(AbstractJBossMarshaller.class);
   protected final MarshallingConfiguration baseCfg;
   protected static final MarshallerFactory factory = new JBossMarshallerFactory();
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
      baseCfg.setCreator(new SunReflectiveCreator());
      baseCfg.setExceptionListener(new DebuggingExceptionListener());
      baseCfg.setClassExternalizerFactory(new SerializeWithExtFactory());
      baseCfg.setVersion(3);
   }

   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      out.writeObject(obj);
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException {
      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(estimatedSize);
      ObjectOutput marshaller = startObjectOutput(baos, false);
      try {
         objectToObjectStream(o, marshaller);
      } finally {
         finishObjectOutput(marshaller);
      }
      return new ByteBuffer(baos.getRawBuffer(), 0, baos.size());
   }

   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant) throws IOException {
      org.jboss.marshalling.Marshaller marshaller = getMarshaller(isReentrant);
      marshaller.start(Marshalling.createByteOutput(os));
      return marshaller;
   }

   protected abstract Marshaller getMarshaller(boolean isReentrant) throws IOException;

   public void finishObjectOutput(ObjectOutput oo) {
      try {
         if (log.isTraceEnabled())
            log.trace("Stop marshaller");

         ((org.jboss.marshalling.Marshaller) oo).finish();
      } catch (IOException ignored) {
      }
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException,
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

   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      Unmarshaller unmarshaller = getUnmarshaller(isReentrant);

      if (log.isTraceEnabled())
         log.tracef("Start unmarshaller after retrieving marshaller from %s",
                   isReentrant ? "factory" : "thread local");

      unmarshaller.start(Marshalling.createByteInput(is));
      return unmarshaller;
   }

   protected abstract Unmarshaller getUnmarshaller(boolean isReentrant) throws IOException;

   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      return in.readObject();
   }

   public void finishObjectInput(ObjectInput oi) {
      try {
         if (log.isTraceEnabled())
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

   protected static class DebuggingExceptionListener implements ExceptionListener {
      private static final URL[] EMPTY_URLS = {};

      @Override
      public void handleMarshallingException(Throwable problem, Object subject) {
         if (log.isDebugEnabled()) {
            TraceInformation.addUserInformation(problem, "toString = " + subject.toString());
         }
      }

      @Override
      public void handleUnmarshallingException(Throwable problem, Class<?> subjectClass) {
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

      private static URL[] getClassLoaderURLs(ClassLoader cl) {
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

}
