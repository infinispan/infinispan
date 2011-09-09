/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.marshall;

import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.io.ByteBuffer;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.marshall.jboss.ExternalizerTable;
import org.infinispan.marshall.jboss.JBossMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * A delegate to various other marshallers like {@link JBossMarshaller}. This delegating marshaller adds versioning
 * information to the stream when marshalling objects and is able to pick the appropriate marshaller to delegate to
 * based on the versioning information when unmarshalling objects.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public class VersionAwareMarshaller extends AbstractMarshaller implements StreamingMarshaller {
   private static final Log log = LogFactory.getLog(VersionAwareMarshaller.class);
   private boolean trace = log.isTraceEnabled();

//   private static final int VERSION_400 = 400;
//   private static final int VERSION_410 = 410;
//   private static final int VERSION_500 = 500;
   private static final int VERSION_510 = 510;

   private final JBossMarshaller defaultMarshaller;
   private ClassLoader loader;
   private InvocationContextContainer icc;
   private String cacheName;

   public VersionAwareMarshaller() {
      defaultMarshaller = new JBossMarshaller();
   }

   public void inject(Configuration cfg, ClassLoader loader, InvocationContextContainer icc, ExternalizerTable extTable) {
      if (cfg == null) {
         this.loader = loader;
         this.cacheName = null;
      } else {
         this.loader = cfg.getClassLoader();
         this.cacheName = cfg.getName();
      }

      this.icc = icc;
      this.defaultMarshaller.inject(extTable, this.loader, this.icc);
   }

   public void stop() {
      defaultMarshaller.stop();
   }

   @Override
   protected ByteBuffer objectToBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(estimatedSize);
      ObjectOutput out = startObjectOutput(baos, false);
      try {
         defaultMarshaller.objectToObjectStream(obj, out);
      } catch (java.io.NotSerializableException nse) {
         if (log.isDebugEnabled()) log.debug("Object is not serializable", nse);
         throw new org.infinispan.marshall.NotSerializableException(nse.getMessage(), nse.getCause());
      } catch (IOException ioe) {
         if (ioe.getCause() instanceof InterruptedException) {
            if (log.isTraceEnabled()) log.trace("Interrupted exception while marshalling", ioe.getCause());
            throw (InterruptedException) ioe.getCause();
         } else {
            log.errorMarshallingObject(ioe);
            throw ioe;
         }
      } finally {
         finishObjectOutput(out);
      }
      return new ByteBuffer(baos.getRawBuffer(), 0, baos.size());
   }

   @Override
   public Object objectFromByteBuffer(byte[] bytes, int offset, int len) throws IOException, ClassNotFoundException {
      ByteArrayInputStream is = new ByteArrayInputStream(bytes, offset, len);
      ObjectInput in = startObjectInput(is, false);
      Object o = null;
      try {
         o = defaultMarshaller.objectFromObjectStream(in);
      } finally {
         finishObjectInput(in);
      }
      return o;
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant) throws IOException {
      ObjectOutput out = defaultMarshaller.startObjectOutput(os, isReentrant);
      try {
         final int version = VERSION_510;
         out.writeShort(version);
         if (trace) log.tracef("Wrote version %s", version);
      } catch (Exception e) {
         finishObjectOutput(out);
         log.unableToReadVersionId();
         throw new IOException("Unable to read version id from first two bytes of stream : " + e.getMessage());
      }
      return out;
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      defaultMarshaller.finishObjectOutput(oo);
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      /* No need to write version here. Clients should either be calling either:
       * - startObjectOutput() -> objectToObjectStream() -> finishObjectOutput()  
       * or
       * - objectToBuffer() // underneath it calls start/finish
       * So, there's only need to write version during the start. 
       * First option is preferred when multiple objects are gonna be written.
       */
      defaultMarshaller.objectToObjectStream(obj, out);
   }

   @Override   
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      ObjectInput in = defaultMarshaller.startObjectInput(is, isReentrant);
      int versionId;
      try {
         versionId = in.readShort();
         if (trace) log.tracef("Read version %s", versionId);
      }
      catch (Exception e) {
         finishObjectInput(in);
         log.unableToReadVersionId();
         throw new IOException("Unable to read version id from first two bytes of stream: " + e.getMessage());
      }
      return in;
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      defaultMarshaller.finishObjectInput(oi);
   }

   @Override   
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      /* No need to read version here. Clients should either be calling either:
       * - startObjectInput() -> objectFromObjectStream() -> finishObjectInput()
       * or
       * - objectFromByteBuffer() // underneath it calls start/finish
       * So, there's only need to read version during the start. 
       * First option is preferred when multiple objects are gonna be written.
       */
      try {
         return defaultMarshaller.objectFromObjectStream(in);
      } catch (EOFException e) {
         IOException ee = new EOFException(
            "The stream ended unexpectedly.  Please check whether the source of " +
               "the stream encountered any issues generating the stream.");
         ee.initCause(e);
         throw ee;
      } catch (IOException ioe) {
         if (trace) log.trace("Log exception reported", ioe); 
         if (ioe.getCause() instanceof InterruptedException)
            throw (InterruptedException) ioe.getCause();
         else
            throw ioe;
      }
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return defaultMarshaller.isMarshallable(o);
   }

   public String getCacheName() {
      return cacheName;
   }
}
