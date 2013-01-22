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

package org.infinispan.lucene.logging;

import java.io.IOException;

import org.infinispan.CacheException;
import org.infinispan.loaders.CacheLoaderException;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import static org.jboss.logging.Logger.Level.*;

/**
 * Log abstraction for the lucene directory. For this module, message ids
 * ranging from 15001 to 16000 inclusively have been reserved.
 *
 * @author Sanne Grinovero
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = ERROR)
   @Message(value = "Error in suspending transaction", id = 15001)
   void errorSuspendingTransaction(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to start transaction", id = 15002)
   void unableToStartTransaction(@Cause Exception e);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to commit work done", id = 15003)
   void unableToCommitTransaction(@Cause Exception e);

   @Message(value = "Unexpected format of key in String form: '%s'", id = 15004)
   IllegalArgumentException keyMappperUnexpectedStringFormat(String key);

   @LogMessage(level = DEBUG)
   @Message(value = "Lucene CacheLoader is ignoring key '%s'", id = 15005)
   void cacheLoaderIgnoringKey(Object key);

   @Message(value = "The LuceneCacheLoader requires a directory; invalid path '%s'", id = 15006)
   CacheException rootDirectoryIsNotADirectory(String fileRoot);

   @Message(value = "LuceneCacheLoader was unable to create the root directory at path '%s'", id = 15007)
   CacheException unableToCreateDirectory(String fileRoot);

   @Message(value = "IOException happened in the CacheLoader", id = 15008)
   CacheLoaderException exceptionInCacheLoader(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Unable to close FSDirectory", id = 15009)
   void errorOnFSDirectoryClose(@Cause IOException e);

   @LogMessage(level = WARN)
   @Message(value = "Error happened while looking for FSDirectories in '%s'", id = 15010)
   void couldNotWalkDirectory(String name, @Cause CacheLoaderException e);

   @LogMessage(level = WARN)
   @Message(value = "The configured autoChunkSize is too small for segment file %s as it is %d bytes; auto-scaling chunk size to %d", id = 15011)
   void rescalingChunksize(String fileName, long fileLength, int chunkSize);

}
