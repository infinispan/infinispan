/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.marshall;

import org.infinispan.EmbeddedMetadata;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.Flag;
import org.infinispan.io.ExposedByteArrayOutputStream;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jgroups.stack.IpAddress;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;

import static org.infinispan.test.TestingUtil.extractCacheMarshaller;
import static org.infinispan.test.TestingUtil.extractGlobalMarshaller;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Test to verify whether the same stream can be used with different marshallers.
 * The test does work but it requires some special treatment. This could be a
 * more efficient way of dealing with global/cache marshaller transition.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "marshall.SharedStreamMultiMarshallerTest", enabled = true)
public class SharedStreamMultiMarshallerTest {

   public void testSharingStream() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager();
      cm.getCache(); // Start cache so that global marshaller is resolved
      JGroupsAddress address = new JGroupsAddress(new IpAddress(12345));
      PutKeyValueCommand cmd = new PutKeyValueCommand(
            "k", "v", false, null, new EmbeddedMetadata.Builder().build(), Collections.<Flag>emptySet());
      try {
         // Write
         StreamingMarshaller globalMarshal = extractGlobalMarshaller(cm);
         ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(1024);
         ObjectOutput globalOO = globalMarshal.startObjectOutput(baos, false, 1024);
         try {
            globalOO.writeObject(address);
            /** BEGIN: Special treatment **/
            globalOO.flush(); // IMPORTANT: Flush needed to make sure the address gets written!!
            globalOO.writeInt(baos.size()); // Note amount of bytes that have been read so far
            globalOO.flush(); // IMPORTANT: Flush again!
            /** END: Special treatment **/

            // Now try cache marshaller to 'borrow' the output stream
            StreamingMarshaller cacheMarshaller = extractCacheMarshaller(cm.getCache());
            ObjectOutput cacheOO = cacheMarshaller.startObjectOutput(baos, true, 1024);
            try {
               cacheOO.writeObject(cmd);
            } finally {
               cacheMarshaller.finishObjectOutput(cacheOO);
            }
         } finally {
            globalMarshal.finishObjectOutput(globalOO);
         }

         byte[] bytes = new byte[baos.size()];
         System.arraycopy(baos.getRawBuffer(), 0, bytes, 0, bytes.length);

         // Read
         ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         ObjectInput globalOI = globalMarshal.startObjectInput(bais, false);
         try {
            assertEquals(address, globalOI.readObject());

            /** BEGIN: Special treatment **/
            int offset = globalOI.readInt();
            // Now try the cache marshaller and borrow the input stream to read
            StreamingMarshaller cacheMarshaller = extractCacheMarshaller(cm.getCache());
            // Advance 4 bytes to go over the number of bytes written
            bais = new ByteArrayInputStream(bytes, offset + 4, bytes.length);
            ObjectInput cacheOI = cacheMarshaller.startObjectInput(bais, true);
            /** END: Special treatment **/

            try {
               assertEquals(cmd, cacheOI.readObject());
            } finally {
               cacheMarshaller.finishObjectInput(cacheOI);
            }
         } finally {
            globalMarshal.finishObjectInput(globalOI);
         }
      } finally {
         cm.stop();
      }
   }

   public void testIndividualStream() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager();
      cm.getCache(); // Start cache so that global marshaller is resolved
      JGroupsAddress address = new JGroupsAddress(new IpAddress(12345));
      try {
         // Write
         StreamingMarshaller globalMarshal = extractGlobalMarshaller(cm);
         ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream(1024);
         ObjectOutput globalOO = globalMarshal.startObjectOutput(baos, false, 1024);
         try {
            globalOO.writeObject(address);
         } finally {
            globalMarshal.finishObjectOutput(globalOO);
         }

         byte[] bytes = new byte[baos.size()];
         System.arraycopy(baos.getRawBuffer(), 0, bytes, 0, bytes.length);

         // Read
         ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         ObjectInput globalOI = globalMarshal.startObjectInput(bais, false);
         try {
            assertEquals(address, globalOI.readObject());
         } finally {
            globalMarshal.finishObjectInput(globalOI);
         }
      } finally {
         cm.stop();
      }
   }

}
