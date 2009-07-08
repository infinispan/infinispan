/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.marshall;

import org.infinispan.commands.RemoteCommandFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.io.ByteBuffer;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.marshall.jboss.JBossMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ByteArrayInputStream;
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
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Galder Zamarreño
 * @since 4.0
 */
public class VersionAwareMarshaller extends AbstractMarshaller {
   private static final Log log = LogFactory.getLog(VersionAwareMarshaller.class);
   private boolean trace = log.isTraceEnabled();

   private static final int VERSION_400 = 400;
   private static final int CUSTOM_MARSHALLER = 999;

   private final JBossMarshaller defaultMarshaller;
   private ClassLoader loader;
   private RemoteCommandFactory remoteCommandFactory;

   public VersionAwareMarshaller() {
      defaultMarshaller = new JBossMarshaller();
   }

   @Inject
   public void inject(ClassLoader loader, RemoteCommandFactory remoteCommandFactory) {
      this.loader = loader;
      this.remoteCommandFactory = remoteCommandFactory;
   }
   
   @Start
   public void start() {
      defaultMarshaller.start(loader, remoteCommandFactory, this);
   }
   
   @Stop
   public void stop() {
      defaultMarshaller.stop();
   }

   protected int getCustomMarshallerVersionInt() {
      return CUSTOM_MARSHALLER;
   }

   public ByteBuffer objectToBuffer(Object obj) throws IOException {
      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(128);
      ObjectOutput out = startObjectOutput(baos, false);
      try {
         defaultMarshaller.objectToObjectStream(obj, out);
      } finally {
         finishObjectOutput(out);
      }
      return new ByteBuffer(baos.getRawBuffer(), 0, baos.size());
   }

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

   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant) throws IOException {
      ObjectOutput out = defaultMarshaller.startObjectOutput(os, isReentrant);
      try {
         out.writeShort(VERSION_400);
         if (trace) log.trace("Wrote version {0}", VERSION_400);         
      } catch (Exception e) {
         finishObjectOutput(out);
         log.error("Unable to read version id from first two bytes of stream, barfing.");
         throw new IOException("Unable to read version id from first two bytes of stream : " + e.getMessage());
      }
      return out;
   }

   public void finishObjectOutput(ObjectOutput oo) {
      defaultMarshaller.finishObjectOutput(oo);
   }

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

   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      ObjectInput in = defaultMarshaller.startObjectInput(is, isReentrant);
      int versionId;
      try {
         versionId = in.readShort();
         if (trace) log.trace("Read version {0}", versionId);
      }
      catch (Exception e) {
         finishObjectInput(in);
         log.error("Unable to read version id from first two bytes of stream, barfing.");
         throw new IOException("Unable to read version id from first two bytes of stream: " + e.getMessage());
      }
      return in;
   }

   public void finishObjectInput(ObjectInput oi) {
      defaultMarshaller.finishObjectInput(oi);
   }

   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      /* No need to read version here. Clients should either be calling either:
       * - startObjectInput() -> objectFromObjectStream() -> finishObjectInput()
       * or
       * - objectFromByteBuffer() // underneath it calls start/finish
       * So, there's only need to read version during the start. 
       * First option is preferred when multiple objects are gonna be written.
       */
      return defaultMarshaller.objectFromObjectStream(in);
   }

   public byte[] objectToByteBuffer(Object obj) throws IOException {
      ByteBuffer b = objectToBuffer(obj);
      byte[] bytes = new byte[b.getLength()];
      System.arraycopy(b.getBuf(), b.getOffset(), bytes, 0, b.getLength());
      return bytes;
   }

   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return objectFromByteBuffer(buf, 0, buf.length);
   }
}