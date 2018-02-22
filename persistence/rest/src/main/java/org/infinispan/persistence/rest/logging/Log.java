/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2011, Red Hat Middleware LLC, and individual contributors
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

package org.infinispan.persistence.rest.logging;

import static org.jboss.logging.Logger.Level.ERROR;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.persistence.spi.PersistenceException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the rest cache store. For this module, message ids
 * ranging from 22001 to 23000 inclusively have been reserved.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   @LogMessage(level = ERROR)
   @Message(value = "Error executing parallel store task", id = 252)
   void errorExecutingParallelStoreTask(@Cause Throwable cause);

   @Message(value = "HTTP error: %s", id = 22002)
   PersistenceException httpError(String status);

   @Message(value = "HTTP error", id = 22003)
   PersistenceException httpError(@Cause Throwable t);

   @Message(value = "Host not specified", id = 22004)
   CacheConfigurationException hostNotSpecified();

   @Message(value = "Error loading entries from remote server", id = 22005)
   PersistenceException errorLoadingRemoteEntries(@Cause Exception e);
}
