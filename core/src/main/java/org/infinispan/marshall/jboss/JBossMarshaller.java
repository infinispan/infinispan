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
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.io.ByteBuffer;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.marshall.AbstractMarshaller;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.ContextClassResolver;
import org.jboss.marshalling.MarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.Unmarshaller;
import org.jboss.marshalling.reflect.SunReflectiveCreator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * JBossMarshaller.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class JBossMarshaller extends AbstractMarshaller {
   private static final Log log = LogFactory.getLog(JBossMarshaller.class);
   private static final String DEFAULT_MARSHALLER_FACTORY = "org.jboss.marshalling.river.RiverMarshallerFactory";
   //   private static final int VERSION_400 = 400;
   //   private static final int DEFAULT_VERSION = VERSION_400;
   private ClassLoader defaultClassLoader;
   private MarshallingConfiguration configuration;
   private MarshallerFactory factory;
   private MagicNumberClassTable classTable;
   private CustomObjectTable objectTable;
   private ExternalizerClassFactory externalizerFactoryAndObjectTable;
///   private boolean trace;

   @Inject
   public void init(ClassLoader defaultCl, Transport transport) {
      log.debug("Using JBoss Marshalling based marshaller.");

//      trace = log.isTraceEnabled();
      defaultClassLoader = defaultCl;

      try {
         // Todo: Enable different marshaller factories via configuration
         factory = (MarshallerFactory) Util.getInstance(DEFAULT_MARSHALLER_FACTORY);
      } catch (Exception e) {
         throw new CacheException("Unable to load JBoss Marshalling marshaller factory " + DEFAULT_MARSHALLER_FACTORY, e);
      }

      classTable = createMagicNumberClassTable();
      objectTable = createCustomObjectTable();
      externalizerFactoryAndObjectTable = createCustomExternalizerFactory(transport, objectTable);

      configuration = new MarshallingConfiguration();
      configuration.setCreator(new SunReflectiveCreator());
      configuration.setClassTable(classTable);
      configuration.setClassExternalizerFactory(externalizerFactoryAndObjectTable);
      configuration.setObjectTable(objectTable);

      // ContextClassResolver provides same functionality as MarshalledValueInputStream
      configuration.setClassResolver(new ContextClassResolver());
//      // Todo: This is the JBMAR underlying protocol version, don't touch! Think about how to get VAM into JBMAR
//      configuration.setVersion(DEFAULT_VERSION);
   }

   @Stop
   public void stop() {
      // Do not leak classloader when cache is stopped.
      defaultClassLoader = null;
      classTable.stop();
      objectTable.stop();
      externalizerFactoryAndObjectTable.stop();
   }

   public byte[] objectToByteBuffer(Object obj) throws IOException {
      ByteBuffer b = objectToBuffer(obj);
      byte[] bytes = new byte[b.getLength()];
      System.arraycopy(b.getBuf(), b.getOffset(), bytes, 0, b.getLength());
      return bytes;
   }

   public ByteBuffer objectToBuffer(Object o) throws IOException {
      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(128);
      ObjectOutput marshaller = startObjectOutput(baos);
      objectToObjectStream(o, marshaller);
      finishObjectOutput(marshaller);
      return new ByteBuffer(baos.getRawBuffer(), 0, baos.size());
   }

   public ObjectOutput startObjectOutput(OutputStream os) throws IOException {
      org.jboss.marshalling.Marshaller marshaller = factory.createMarshaller(configuration);
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
      ClassLoader toUse = defaultClassLoader;
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
      ObjectInput unmarshaller = startObjectInput(is);
      Object o = objectFromObjectStream(unmarshaller);
      finishObjectInput(unmarshaller);
      return o;
   }

   public ObjectInput startObjectInput(InputStream is) throws IOException {
      Unmarshaller unmarshaller = factory.createUnmarshaller(configuration);
      unmarshaller.start(Marshalling.createByteInput(is));
      return unmarshaller;
   }

   public void finishObjectInput(ObjectInput oi) {
      try {
         ((Unmarshaller) oi).finish();
      } catch (IOException e) {
      }
   }

   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      return in.readObject();
   }

   protected MagicNumberClassTable createMagicNumberClassTable() {
      MagicNumberClassTable classTable = new MagicNumberClassTable();
      classTable.init();
      return classTable;
   }

   protected ExternalizerClassFactory createCustomExternalizerFactory(Transport transport, CustomObjectTable objectTable) {
      ExternalizerClassFactory externalizerFactory = new ExternalizerClassFactory(transport, objectTable);
      externalizerFactory.init();
      return externalizerFactory;
   }

   private CustomObjectTable createCustomObjectTable() {
      CustomObjectTable objectTable = new CustomObjectTable();
      objectTable.init();
      return objectTable;
   }
}
