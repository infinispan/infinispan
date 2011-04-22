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
package org.infinispan.distribution.rehash;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * abstract class that needs to be overridden
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class XAResourceAdapter implements XAResource {
   private static final Xid[] XIDS = new Xid[0];

   public void commit(Xid xid, boolean b) throws XAException {
      // no-op
   }

   public void end(Xid xid, int i) throws XAException {
      // no-op
   }

   public void forget(Xid xid) throws XAException {
      // no-op
   }

   public int getTransactionTimeout() throws XAException {
      return 0;
   }

   public boolean isSameRM(XAResource xaResource) throws XAException {
      return false;
   }

   public int prepare(Xid xid) throws XAException {
      return XA_OK;
   }

   public Xid[] recover(int i) throws XAException {
      return XIDS;
   }

   public void rollback(Xid xid) throws XAException {
      // no-op
   }

   public boolean setTransactionTimeout(int i) throws XAException {
      return false;
   }

   public void start(Xid xid, int i) throws XAException {
      // no-op
   }
}
