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

import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.ReflectionUtil;

import java.io.IOException;
import java.io.InputStream;

/**
 * A specialized form of the {@link GenericJBossMarshaller}, making use of a custom object table for types internal to
 * Infinispan.
 * <p />
 * The reason why this is implemented specially in Infinispan rather than resorting to Java serialization or even the
 * more efficient JBoss serialization is that a lot of efficiency can be gained when a majority of the serialization
 * that occurs has to do with a small set of known types such as {@link org.infinispan.transaction.xa.GlobalTransaction} or
 * {@link org.infinispan.commands.ReplicableCommand}, and class type information can be replaced with simple magic
 * numbers.
 * <p/>
 * Unknown types (typically user data) falls back to JBoss serialization.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class JBossMarshaller extends GenericJBossMarshaller implements StreamingMarshaller {   
   private ConstantObjectTable objectTable;

   public void start(ClassLoader defaultCl, RemoteCommandsFactory cmdFactory, StreamingMarshaller ispnMarshaller) {
      if (log.isDebugEnabled()) log.debug("Using JBoss Marshalling");
      this.defaultCl = defaultCl;
      objectTable = createCustomObjectTable(cmdFactory, ispnMarshaller);
      configuration.setObjectTable(objectTable);
   }

   public void stop() {
      super.stop();
      // Do not leak classloader when cache is stopped.
      defaultCl = null;
      if (objectTable != null) objectTable.stop();
   }

   @Override
   public Object objectFromInputStream(InputStream inputStream) throws IOException, ClassNotFoundException {
      // TODO: available() call commented until https://issues.apache.org/jira/browse/HTTPCORE-199 httpcore-nio issue is fixed.
      // int len = inputStream.available();
      ExposedByteArrayOutputStream bytes = new ExposedByteArrayOutputStream(DEFAULT_BUF_SIZE);
      byte[] buf = new byte[Math.min(DEFAULT_BUF_SIZE, 1024)];
      int bytesRead;
      while ((bytesRead = inputStream.read(buf, 0, buf.length)) != -1) bytes.write(buf, 0, bytesRead);
      return objectFromByteBuffer(bytes.getRawBuffer(), 0, bytes.size());
   }

   @Override
   public boolean isMarshallableCandidate(Object o) {
      return super.isMarshallableCandidate(o) || objectTable.isMarshallableCandidate(o);
   }

   private ConstantObjectTable createCustomObjectTable(RemoteCommandsFactory cmdFactory, StreamingMarshaller ispnMarshaller) {
      ConstantObjectTable objectTable = new ConstantObjectTable();
      objectTable.start(cmdFactory, ispnMarshaller);
      return objectTable;
   }
}
