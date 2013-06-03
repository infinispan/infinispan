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
package org.infinispan.upgrade.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

import org.infinispan.CacheException;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Cause;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

/**
 * Log abstraction for the Rolling Upgrade Tools. For this module, message ids
 * ranging from 20001 to 21000 inclusively have been reserved.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {
   @LogMessage(level = ERROR)
   @Message(value = "Could not register upgrade MBean", id = 20001)
   void jmxRegistrationFailed();

   @LogMessage(level = ERROR)
   @Message(value = "Could not unregister upgrade MBean", id = 20002)
   void jmxUnregistrationFailed();

   @Message(value = "The RemoteCacheStore for cache %s should be configured with hotRodWrapping enabled", id = 20003)
   CacheException remoteStoreNoHotRodWrapping(String cacheName);

   @Message(value = "Could not find migration data in cache %s", id = 20004)
   CacheException missingMigrationData(String name);

   @LogMessage(level = WARN)
   @Message(value = "Could not migrate key %s", id = 20005)
   void keyMigrationFailed(String key, @Cause Throwable cause);
}
