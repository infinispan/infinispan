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

package org.infinispan.loaders.cloud.logging;

import org.infinispan.loaders.CacheLoaderException;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import java.util.Set;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the cloud cache store. For this module, message ids
 * ranging from 7001 to 8000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = WARN)
   @Message(value = "Unable to use configured Cloud Service Location [%s].  " +
         "Available locations for Cloud Service [%s] are %s", id = 7001)
   void unableToConfigureCloudService(String loc, String cloudService, Set keySet);

   @LogMessage(level = INFO)
   @Message(value = "Attempt to load the same cloud bucket (%s) ignored", id = 7002)
   void attemptToLoadSameBucketIgnored(String source);

   @LogMessage(level = WARN)
   @Message(value = "Unable to read blob at %s", id = 7003)
   void unableToReadBlob(String blobName, @Cause CacheLoaderException e);

   @LogMessage(level = WARN)
   @Message(value = "Problems purging", id = 7004)
   void problemsPurging(@Cause Exception e);

}
