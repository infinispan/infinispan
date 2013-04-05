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

package org.infinispan.loaders.remote.logging;

import org.infinispan.config.ConfigurationException;
import org.infinispan.loaders.CacheLoaderException;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the remote cache store. For this module, message ids
 * ranging from 10001 to 11000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = ERROR)
   @Message(value = "RemoteCacheStore can only run in shared mode! This method shouldn't be called in shared mode", id = 10001)
   void sharedModeOnlyAllowed();

   @Message(value = "Wrapper cannot handle values of class %s", id = 10004)
   CacheLoaderException unsupportedValueFormat(String name);

   @Message(value = "Cannot enable HotRod wrapping if a marshaller and/or an entryWrapper have already been set", id = 10005)
   ConfigurationException cannotEnableHotRodWrapping();

   @Message(value = "Cannot load the HotRodEntryWrapper class (make sure the infinispan-server-hotrod classes are available)", id = 10006)
   ConfigurationException cannotLoadHotRodEntryWrapper(@Cause Exception e);
}
