/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.statetransfer;

import org.infinispan.CacheException;

/**
 * An exception signalling that a command should be retried because it was executed with an outdated
 * topology.
 *
 * This can happen for non-tx caches, if the primary owner doesn't respond (either because it left the
 * cluster or because this particular cache is no longer running).
 *
 * @author Dan Berindei
 * @since 6.0
 */
public class OutdatedTopologyException extends CacheException {
   public OutdatedTopologyException(String msg) {
      super(msg);
   }

   public OutdatedTopologyException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
