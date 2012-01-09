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
package org.infinispan.transaction.xa.recovery;

import javax.transaction.xa.Xid;
import java.util.HashSet;
import java.util.List;

/**
*  Default implementation for RecoveryIterator.
*
* @author Mircea.Markus@jboss.com
* @since 5.0
*/
public class PreparedTxIterator implements RecoveryManager.RecoveryIterator {

   private final HashSet<Xid> xids = new HashSet<Xid>(4);

   @Override
   public boolean hasNext() {
      return !xids.isEmpty();
   }

   @Override
   public Xid[] next() {
      Xid[] result = xids.toArray(new Xid[xids.size()]);
      xids.clear();
      return result;
   }

   public void add(List<Xid> xids) {
      this.xids.addAll(xids);
   }

   @Override
   public Xid[] all() {
      return next();
   }

   @Override
   public void remove() {
      throw new RuntimeException("Unsupported operation!");
   }
}
