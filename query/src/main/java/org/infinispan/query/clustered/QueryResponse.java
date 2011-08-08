/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat Middleware LLC, and individual contributors
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

import java.io.Serializable;
import java.util.UUID;

import org.apache.lucene.search.TopDocs;
import org.infinispan.remoting.transport.Address;

/**
 * QueryResponse.
 * 
 * A response of a request to create a new distributed lazy iterator
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class QueryResponse implements Serializable {

   private static final long serialVersionUID = -2113889511877165954L;

   private UUID nodeUUID;

   private TopDocs topDocs;

   private Address address;

   private Integer resultSize;

   private Object fetchedValue;

   public TopDocs getTopDocs() {
      return topDocs;
   }

   public QueryResponse(Object value) {
      fetchedValue = value;
   }

   public QueryResponse(int resultSize) {
      this.resultSize = resultSize;
   }

   public QueryResponse(TopDocs topDocs, UUID nodeUUid, int resultSize) {
      this.nodeUUID = nodeUUid;
      this.topDocs = topDocs;
      this.resultSize = resultSize;
   }

   public int getResultSize() {
      return resultSize;
   }

   public UUID getNodeUUID() {
      return nodeUUID;
   }

   public void setAddress(Address address) {
      this.address = address;
   }

   public Address getAddress() {
      return address;
   }

   public Object getFetchedValue() {
      return fetchedValue;
   }

}