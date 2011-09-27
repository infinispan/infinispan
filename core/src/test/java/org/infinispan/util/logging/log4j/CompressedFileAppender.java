/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.util.logging.log4j;

import org.apache.log4j.FileAppender;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

/**
 * Appender that writes to a file and compresses the output using gzip.
 *
 * Based on <code>org.apache.log4j.FileAppender</code>
 */
public class CompressedFileAppender extends FileAppender {

   GZIPOutputStream gzos;

   @Override
   protected OutputStreamWriter createWriter(OutputStream os) {
      try {
         gzos = new GZIPOutputStream(os, bufferSize);
         gzos.flush();
         return super.createWriter(gzos);
      } catch (IOException e) {
         throw new RuntimeException("Unable to create gzipped output stream");
      }
   }

   @Override
   protected void reset() {
      if (gzos != null) {
         closeCompressor();
         gzos = null;
      }
      super.reset();
   }

   private void closeCompressor() {
      try {
         gzos.close();
      } catch (IOException e) {
         throw new RuntimeException("Unable to finish gzipped output stream");
      }
   }
}
