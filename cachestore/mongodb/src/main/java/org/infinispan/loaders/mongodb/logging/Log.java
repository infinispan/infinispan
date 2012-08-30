/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat Middleware LLC, and individual contributors
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

package org.infinispan.loaders.mongodb.logging;

import com.mongodb.MongoException;
import org.infinispan.loaders.CacheLoaderException;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.INFO;

/**
 * Log abstraction for the mongodb store. For this module, message ids ranging from 21001 to 22000 inclusively have been
 * reserved.
 *
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = INFO)
   @Message(id = 21001, value = "Connecting to MongoDB at %1$s:%2$d with a timeout set at %3$d millisecond(s) and an acknowledgment to %4$d")
   void connectingToMongo(String host, int port, int timeout, int acknowledgment);

   @LogMessage(level = INFO)
   @Message(id = 21002, value = "Closing connection to MongoDB sever instance")
   void disconnectingFromMongo();

   @Message(id = 21003, value = "Unable to find or initialize a connection to the MongoDB server")
   CacheLoaderException unableToInitializeMongoDB(@Cause RuntimeException e);

   @Message(id = 21004, value = "The value set for the configuration property  must be a number between 1 and 65535. Found '[%s]'.")
   CacheLoaderException mongoPortIllegalValue(Object value);

   @Message(id = 21005, value = "Could not resolve MongoDB hostname [%s]")
   CacheLoaderException mongoOnUnknownHost(String hostname);

   @LogMessage(level = INFO)
   @Message(id = 21006, value = "Mongo database named [%s] is not defined. Creating it!")
   void creatingDatabase(String dbName);

   @LogMessage(level = INFO)
   @Message(id = 21007, value = "Connecting to Mongo database named [%s].")
   void connectingToMongoDatabase(String dbName);

   @Message(id = 21008, value = "The database name was not set. Can't connect to MongoDB.")
   CacheLoaderException mongoDbNameMissing();

   @Message(id = 21009, value = "MongoDB authentication failed with username [%s]")
   CacheLoaderException authenticationFailed(String username);

   @Message(id = 21010, value = "Unable to connect to MongoDB instance %1$s:%2$d")
   CacheLoaderException unableToConnectToDatastore(String host, int port, @Cause Exception e);

   @Message(id = 21011, value = "Unable to unmarshall [%s]")
   CacheLoaderException unableToUnmarshall(Object o, @Cause Exception e);

   @Message(id = 21012, value = "Unable to marshall")
   CacheLoaderException unableToUnmarshall(@Cause Exception e);

   @Message(id = 21013, value = "Unable to unmarshall")
   CacheLoaderException unableToMarshall(@Cause Exception e);

   @LogMessage(level = DEBUG)
   @Message(id = 21014, value = "Running tests on [%1$s : %2$d]")
   void runningTest(String host, int port);

   @Message(id = 21015, value = "Unable to load [%s]")
   CacheLoaderException unableToFindFromDatastore(String id, @Cause MongoException e);
}