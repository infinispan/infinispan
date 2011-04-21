/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.io.ExposedByteArrayOutputStream;

import java.io.*;

/**
 * Abstract marshaller
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractStreamingMarshaller extends AbstractMarshaller implements StreamingMarshaller {

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

}
