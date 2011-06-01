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
package org.infinispan.query.clustered;

import java.util.UUID;

import org.apache.lucene.search.TopDocs;
import org.infinispan.remoting.transport.Address;

/**
 * ClusteredTopDocs.
 * 
 * A TopDocs with UUID and address of node who has the doc.
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class ClusteredTopDocs {

   private int currentIndex = 0;

   private final TopDocs topDocs;

   private final UUID id;

   private Address nodeAddress;

   ClusteredTopDocs(TopDocs topDocs, UUID id) {
      this.topDocs = topDocs;
      this.id = id;
   }

   public UUID getId() {
      return id;
   }

   public boolean hasNext() {
      return !(currentIndex >= topDocs.scoreDocs.length);
   } 

   public ClusteredFieldDoc getNext() {
      if (currentIndex >= topDocs.scoreDocs.length)
         return null;

      ClusteredFieldDoc doc =  new ClusteredFieldDoc(topDocs.scoreDocs[currentIndex], id, currentIndex);
      currentIndex++;
      return doc;
   }

   public void setNodeAddress(Address nodeAddress) {
      this.nodeAddress = nodeAddress;
   }

   public Address getNodeAddress() {
      return nodeAddress;
   }
}