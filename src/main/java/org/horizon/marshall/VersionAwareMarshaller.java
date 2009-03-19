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
package org.horizon.marshall;

import org.horizon.commands.RemoteCommandFactory;
import org.horizon.factories.annotations.Inject;
import org.horizon.io.ByteBuffer;
import org.horizon.io.ExposedByteArrayOutputStream;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.jboss.util.stream.MarshalledValueInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * A delegate to various other marshallers like {@link HorizonMarshaller}. This delegating marshaller adds versioning
 * information to the stream when marshalling objects and is able to pick the appropriate marshaller to delegate to
 * based on the versioning information when unmarshalling objects.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
public class VersionAwareMarshaller implements Marshaller {
   private static final Log log = LogFactory.getLog(VersionAwareMarshaller.class);
   private boolean trace = log.isTraceEnabled();

   private static final int VERSION_100 = 100;
   private static final int CUSTOM_MARSHALLER = 999;

   private HorizonMarshaller defaultMarshaller;

   ClassLoader defaultClassLoader;

   @Inject
   public void init(ClassLoader loader, RemoteCommandFactory remoteCommandFactory) {
      defaultMarshaller = new HorizonMarshaller();
      defaultMarshaller.init(loader, remoteCommandFactory);
   }

   protected int getCustomMarshallerVersionInt() {
      return CUSTOM_MARSHALLER;
   }

   public ByteBuffer objectToBuffer(Object obj) throws IOException {
      ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(128);
      ObjectOutputStream out = new ObjectOutputStream(baos);

      out.writeShort(VERSION_100);
      log.trace("Wrote version {0}", VERSION_100);

      //now marshall the contents of the object
      defaultMarshaller.objectToObjectStream(obj, out);
      out.close();

      // and return bytes.
      return new ByteBuffer(baos.getRawBuffer(), 0, baos.size());
   }

   public Object objectFromByteBuffer(byte[] bytes, int offset, int len) throws IOException, ClassNotFoundException {
      int versionId;
      ObjectInputStream in = new MarshalledValueInputStream(new ByteArrayInputStream(bytes, offset, len));
      try {
         versionId = in.readShort();
         log.trace("Read version {0}", versionId);
      }
      catch (Exception e) {
         log.error("Unable to read version id from first two bytes of stream, barfing.");
         throw new IOException("Unable to read version id from first two bytes of stream.");
      }
      return defaultMarshaller.objectFromObjectStream(in);
   }

   public Object objectFromStream(InputStream is) throws IOException, ClassNotFoundException {
      short versionId;
      ObjectInputStream in = new MarshalledValueInputStream(is);
      try {
         versionId = in.readShort();
         if (trace) log.trace("Read version " + versionId);
      }
      catch (Exception e) {
         log.error("Unable to read version id from first two bytes of stream, barfing.");
         throw new IOException("Unable to read version id from first two bytes of stream.");
      }
      return defaultMarshaller.objectFromObjectStream(in);
   }

   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      out.writeShort(VERSION_100);
      log.trace("Wrote version {0}", VERSION_100);
      defaultMarshaller.objectToObjectStream(obj, out);
   }

   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      int versionId;
      try {
         versionId = in.readShort();
         log.trace("Read version {0}", versionId);
      }
      catch (Exception e) {
         log.error("Unable to read version id from first two bytes of stream, barfing.");
         throw new IOException("Unable to read version id from first two bytes of stream.");
      }
      return defaultMarshaller.objectFromObjectStream(in);
   }

   public byte[] objectToByteBuffer(Object obj) throws IOException {
      return defaultMarshaller.objectToByteBuffer(obj);
   }

   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return defaultMarshaller.objectFromByteBuffer(buf);
   }
}