package org.infinispan.marshall.jboss;

import org.infinispan.CacheException;
import org.infinispan.io.ByteBuffer;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.marshall.AbstractMarshaller;
import org.infinispan.util.ConcurrentWeakKeyHashMap;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.ContextClassResolver;
import org.jboss.marshalling.ExceptionListener;
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
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A marshaller that makes use of <a href="http://www.jboss.org/jbossmarshalling">JBoss Marshalling</a> to serialize
 * and deserialize objects.
 * <p />
 * In addition to making use of JBoss Marshalling, this Marshaller 
 * @author Manik Surtani
 * @version 4.1
 * @see <a href="http://www.jboss.org/jbossmarshalling">JBoss Marshalling</a>
 */
public class GenericJBossMarshaller extends AbstractMarshaller {

   protected static final Log log = LogFactory.getLog(JBossMarshaller.class);
   protected ClassLoader defaultCl = this.getClass().getClassLoader();
   protected MarshallingConfiguration configuration;
   protected MarshallerFactory factory;
   /**
    * Cache of classes that are considered to be marshallable. Since checking
    * whether a type is marshallable requires attempting to marshalling them,
    * a cache for the types that are known to be marshallable or not is
    * advantageous.
    */
   private final ConcurrentMap<Class, Boolean> isMarshallableMap = new ConcurrentWeakKeyHashMap<Class, Boolean>();

   public GenericJBossMarshaller() {
      factory = Marshalling.getMarshallerFactory("river");

      configuration = new MarshallingConfiguration();
      configuration.setCreator(new SunReflectiveCreator());
      configuration.setExceptionListener(new DebuggingExceptionListener());
      // ContextClassResolver provides same functionality as MarshalledValueInputStream
      configuration.setClassResolver(new ContextClassResolver());
      configuration.setVersion(2);

   }

   /**
    * Marshaller thread local. JBossMarshaller is a singleton shared by all caches (global component), so no urgent need
    * for static here. JBMAR clears pretty much any state during finish(), so no urgent need to clear the thread local
    * since it shouldn't be leaking.
    */
   private ThreadLocal<org.jboss.marshalling.Marshaller> marshallerTL = new ThreadLocal<org.jboss.marshalling.Marshaller>() {
      @Override
      protected org.jboss.marshalling.Marshaller initialValue() {
         try {
            return factory.createMarshaller(configuration);
         } catch (IOException e) {
            throw new CacheException(e);
         }
      }
   };

   /**
    * Unmarshaller thread local. JBossMarshaller is a singleton shared by all caches (global component), so no urgent
    * need for static here. JBMAR clears pretty much any state during finish(), so no urgent need to clear the thread
    * local since it shouldn't be leaking.
    */
   private ThreadLocal<Unmarshaller> unmarshallerTL = new ThreadLocal<Unmarshaller>() {
      @Override
      protected Unmarshaller initialValue() {
         try {
            return factory.createUnmarshaller(configuration);
         } catch (IOException e) {
            throw new CacheException(e);
         }
      }
   };

   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      ClassLoader toUse = defaultCl;
      Thread current = Thread.currentThread();
      ClassLoader old = current.getContextClassLoader();
      if (old != null) toUse = old;

      try {
         current.setContextClassLoader(toUse);
         out.writeObject(obj);
      }
      finally {
         current.setContextClassLoader(old);
      }
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
      org.jboss.marshalling.Marshaller marshaller;
      if (isReentrant) {
         marshaller = factory.createMarshaller(configuration);
      } else {
         marshaller = marshallerTL.get();
      }

      if (log.isTraceEnabled())
         log.trace("Start marshaller after retrieving marshaller from %s",
                   isReentrant ? "factory" : "thread local");

      marshaller.start(Marshalling.createByteOutput(os));
      return marshaller;
   }

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
      Unmarshaller unmarshaller;
      if (isReentrant) {
         unmarshaller = factory.createUnmarshaller(configuration);
      } else {
         unmarshaller = unmarshallerTL.get();
      }

      if (log.isTraceEnabled())
         log.trace("Start unmarshaller after retrieving marshaller from %s",
                   isReentrant ? "factory" : "thread local");

      unmarshaller.start(Marshalling.createByteInput(is));
      return unmarshaller;
   }

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
   public boolean isMarshallable(Object o) {
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
            } finally {
               isMarshallableMap.putIfAbsent(clazz, isMarshallable);
               return isMarshallable;
            }
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
      private static final Class[] EMPTY_CLASSES = {};
      private static final Object[] EMPTY_OBJECTS = {};

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
            Class[] parameterTypes = EMPTY_CLASSES;
            Method getURLs = cl.getClass().getMethod("getURLs", parameterTypes);
            if (returnType.isAssignableFrom(getURLs.getReturnType())) {
               Object[] args = EMPTY_OBJECTS;
               urls = (URL[]) getURLs.invoke(cl, args);
            }
         } catch (Exception ignore) {
         }
         return urls;
      }

   }   
}
