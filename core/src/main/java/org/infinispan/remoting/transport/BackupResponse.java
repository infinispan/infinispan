/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.remoting.transport;

import java.util.Map;
import java.util.Set;

/**
 * Represents a response from a backup replication call.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public interface BackupResponse {

   void waitForBackupToFinish() throws Exception;

   Map<String,Throwable> getFailedBackups();

   /**
    * Returns the list of sites where the backups failed due to a bridge communication error (as opposed to an
    * error caused by Infinispan, e.g. due to a lock acquisition timeout).
    */
   Set<String> getCommunicationErrors();

   /**
    * Return the time in millis when this operation was initiated.
    */
   long getSendTimeMillis();

   boolean isEmpty();
}
