/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.marshall.jboss;

import org.infinispan.CacheException;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.io.ByteBuffer;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.marshall.AbstractStreamingMarshaller;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.Util;
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
import java.lang.reflect.Method;
import java.net.URL;

/**
 * The reason why this is implemented specially in Infinispan rather than resorting to Java serialization or even the
 * more efficient JBoss serialization is that a lot of efficiency can be gained when a majority of the serialization
 * that occurs has to do with a small set of known types such as {@link org.infinispan.transaction.GlobalTransaction} or
 * {@link org.infinispan.commands.ReplicableCommand}, and class type information can be replaced with simple magic
 * numbers.
 * <p/>
 * Unknown types (typically user data) falls back to JBoss serialization.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class JBossMarshaller extends AbstractStreamingMarshaller {
   private static final Log log = LogFactory.getLog(JBossMarshaller.class);
   private static final String DEFAULT_MARSHALLER_FACTORY = "org.jboss.marshalling.river.RiverMarshallerFactory";
   private ClassLoader defaultCl;
   private MarshallingConfiguration configuration;
   private MarshallerFactory factory;
   private ConstantObjectTable objectTable;

   /**
    * StreamingMarshaller thread local. JBossMarshaller is a singleton shared by all caches (global component), so no urgent need
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

   public void start(ClassLoader defaultCl, RemoteCommandsFactory cmdFactory, StreamingMarshaller ispnMarshaller) {
      if (log.isDebugEnabled()) log.debug("Using JBoss Marshalling");
      this.defaultCl = defaultCl;
      factory = (MarshallerFactory) Util.getInstance(DEFAULT_MARSHALLER_FACTORY);

      objectTable = createCustomObjectTable(cmdFactory, ispnMarshaller);
      configuration = new MarshallingConfiguration();
      configuration.setCreator(new SunReflectiveCreator());
      configuration.setObjectTable(objectTable);
      configuration.setExceptionListener(new DebuggingExceptionListener());
      // ContextClassResolver provides same functionality as MarshalledValueInputStream
      configuration.setClassResolver(new ContextClassResolver());
      configuration.setVersion(2);
   }

   public void stop() {
      // Do not leak classloader when cache is stopped.
      defaultCl = null;
      if (objectTable != null) objectTable.stop();
   }

   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException {
      ByteBuffer b = objectToBuffer(obj, estimatedSize);
      byte[] bytes = new byte[b.getLength()];
      System.arraycopy(b.getBuf(), b.getOffset(), bytes, 0, b.getLength());
      return bytes;
   }

   public ByteBuffer objectToBuffer(Object o) throws IOException {
      return objectToBuffer(o, DEFAULT_BUF_SIZE);
   }

   private ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException {
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
      marshaller.start(Marshalling.createByteOutput(os));
      return marshaller;
   }

   public void finishObjectOutput(ObjectOutput oo) {
      try {
         ((org.jboss.marshalling.Marshaller) oo).finish();
      } catch (IOException ioe) {
      }
   }

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

   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return objectFromByteBuffer(buf, 0, buf.length);
   }

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
      unmarshaller.start(Marshalling.createByteInput(is));
      return unmarshaller;
   }

   public void finishObjectInput(ObjectInput oi) {
      try {
         if (oi != null) ((Unmarshaller) oi).finish();
      } catch (IOException e) {
      }
   }

   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      return in.readObject();
   }

   private ConstantObjectTable createCustomObjectTable(RemoteCommandsFactory cmdFactory, StreamingMarshaller ispnMarshaller) {
      ConstantObjectTable objectTable = new ConstantObjectTable();
      objectTable.start(cmdFactory, ispnMarshaller);
      return objectTable;
   }

   private static class DebuggingExceptionListener implements ExceptionListener {
      private static final URL[] EMPTY_URLS = {};
      private static final Class[] EMPTY_CLASSES = {};
      private static final Object[] EMPTY_OBJECTS = {};

      public void handleMarshallingException(Throwable problem, Object subject) {
         if (log.isDebugEnabled()) {
            TraceInformation.addUserInformation(problem, "toString = " + subject.toString());
         }
      }

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
