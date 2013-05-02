/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.mongodb;

import com.mongodb.*;
import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.mongodb.logging.Log;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A persistent <code>CacheLoader</code> based on MongoDB. See http://www.mongodb.org/
 *
 * @author Guillaume Scheibel <guillaume.scheibel@gmail.com>
 */
@ThreadSafe
@CacheLoaderMetadata(configurationClass = MongoDBCacheStoreConfig.class)
public class MongoDBCacheStore extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(MongoDBCacheStore.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private MongoDBCacheStoreConfig cfg;
   private MongoClient mongo;
   private DBCollection collection;
   private DB mongoDb;
   private static final String ID_FIELD = "_id";
   private static final String TIMESTAMP_FIELD = "timestamp";
   private static final String VALUE_FIELD = "value";

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.cfg = (MongoDBCacheStoreConfig) config;
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      BasicDBObject searchObject = new BasicDBObject();
      searchObject.put(TIMESTAMP_FIELD, new BasicDBObject("$gt", 0).append("$lt", timeService.wallClockTime()));
      this.collection.remove(searchObject);
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      if (trace) {
         log.tracef("Adding entry: %s", entry);
      }
      byte[] id = objectToByteBuffer(entry.getKey());
      if (this.findById(id) == null) {
         BasicDBObject entryObject = this.createDBObject(id);
         entryObject.put(VALUE_FIELD, objectToByteBuffer(entry));
         entryObject.put(TIMESTAMP_FIELD, entry.getExpiryTime());
         this.collection.update(entryObject, entryObject, true, false);
      } else {
         BasicDBObject updater = new BasicDBObject(VALUE_FIELD, objectToByteBuffer(entry));
         updater.append(TIMESTAMP_FIELD, entry.getExpiryTime());
         this.collection.update(this.createDBObject(id), updater);
      }
   }

   private BasicDBObject createDBObject(Object key) {
      BasicDBObject dbObject = new BasicDBObject();
      dbObject.put(ID_FIELD, key);
      return dbObject;
   }

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      Object objectFromStream;
      try {
         objectFromStream = getMarshaller().objectFromObjectStream(inputStream);
      } catch (IOException e) {
         Thread.currentThread().interrupt();
         throw log.unableToUnmarshall(e);
      } catch (ClassNotFoundException e) {
         throw log.unableToUnmarshall(e);
      } catch (InterruptedException e) {
         throw log.unableToUnmarshall(e);
      }
      if (objectFromStream instanceof InternalCacheEntry) {
         InternalCacheEntry ice = (InternalCacheEntry) objectFromStream;
         this.store(ice);
      } else if (objectFromStream instanceof Set) {
         Set<InternalCacheEntry> internalCacheEntries = (Set<InternalCacheEntry>) objectFromStream;
         for (InternalCacheEntry ice : internalCacheEntries) {
            this.store(ice);
         }
      }
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      try {
         Set<InternalCacheEntry> internalCacheEntries = this.loadAll();
         if (internalCacheEntries.size() == 1) {
            getMarshaller().objectToObjectStream(internalCacheEntries.iterator().next(), outputStream);
         } else if (internalCacheEntries.size() > 1) {
            getMarshaller().objectToObjectStream(internalCacheEntries, outputStream);
         }
      } catch (Exception e) {
         throw log.unableToMarshall(e);
      }
   }

   @Override
   public void clear() {
      this.collection.drop();
   }

   private DBObject findById(byte[] id) throws CacheLoaderException {
      try {
         return this.collection.findOne(new BasicDBObject(ID_FIELD, id));
      } catch (MongoException e) {
         throw log.unableToFindFromDatastore(id.toString(), e);
      }
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      byte[] id = objectToByteBuffer(key);
      if (this.findById(id) == null) {
         return false;
      } else {
         this.collection.remove(this.createDBObject(id));
         return true;
      }
   }

   private Object unmarshall(DBObject dbObject, String field) throws CacheLoaderException {
      try {
         return getMarshaller().objectFromByteBuffer((byte[]) dbObject.get(field));
      } catch (IOException e) {
         throw log.unableToUnmarshall(dbObject, e);
      } catch (ClassNotFoundException e) {
         throw log.unableToUnmarshall(dbObject, e);
      }
   }

   private InternalCacheEntry createInternalCacheEntry(DBObject dbObject) throws CacheLoaderException {
      return (InternalCacheEntry) unmarshall(dbObject, VALUE_FIELD);
   }

   private byte[] objectToByteBuffer(Object key) throws CacheLoaderException {
      try {
         return getMarshaller().objectToByteBuffer(key);
      } catch (IOException e) {
         throw log.unableToUnmarshall(key, e);
      } catch (InterruptedException e) {
         throw log.unableToUnmarshall(key, e);
      }
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      byte[] id = objectToByteBuffer(key);
      BasicDBObject dbObject = this.createDBObject(id);
      DBObject[] orArray = new BasicDBObject[2];
      orArray[0] = new BasicDBObject(TIMESTAMP_FIELD, new BasicDBObject("$gte", timeService.wallClockTime()));
      orArray[1] = new BasicDBObject(TIMESTAMP_FIELD, -1);
      dbObject.append("$or", orArray);
      DBObject rawResult = this.collection.findOne(dbObject);
      if (rawResult != null) {
         return this.createInternalCacheEntry(rawResult);
      } else {
         return null;
      }
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      DBCursor dbObjects = this.collection.find();
      Set<InternalCacheEntry> entries = new HashSet<InternalCacheEntry>(dbObjects.count());
      while (dbObjects.hasNext()) {
         DBObject next = dbObjects.next();
         InternalCacheEntry ice = this.createInternalCacheEntry(next);
         entries.add(ice);
      }
      return entries;
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      Set<InternalCacheEntry> values = new HashSet<InternalCacheEntry>();
      DBCursor dbObjects = this.collection.find().limit(numEntries);
      for (DBObject dbObject : dbObjects) {
         InternalCacheEntry ice = this.createInternalCacheEntry(dbObject);
         values.add(ice);
      }
      return values;
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      BasicDBObject excludeObject = new BasicDBObject();
      Set<Object> values = new HashSet<Object>();
      DBCursor keyObjects = null;
      BasicDBObject idRestrictionObject = new BasicDBObject(ID_FIELD, 1);
      if (keysToExclude != null) {
         byte[][] exclusionArray = new byte[keysToExclude.size()][];
         Iterator<Object> iterator = keysToExclude.iterator();
         int i = 0;

         while (iterator.hasNext()) {
            Object next = iterator.next();
            exclusionArray[i++] = objectToByteBuffer(next);
         }
         excludeObject.put(ID_FIELD, new BasicDBObject("$nin", exclusionArray));
         keyObjects = this.collection.find(excludeObject);
      } else {
         keyObjects = this.collection.find(new BasicDBObject(), idRestrictionObject);
      }
      for (DBObject rawObject : keyObjects) {
         values.add(this.unmarshall(rawObject, ID_FIELD));
      }
      return values;
   }

   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return MongoDBCacheStoreConfig.class;
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      try {
         MongoClientOptions.Builder optionBuilder = new MongoClientOptions.Builder();
         optionBuilder.connectTimeout(this.cfg.getTimeout());

         WriteConcern writeConcern = new WriteConcern(this.cfg.getAcknowledgment());
         optionBuilder.writeConcern(writeConcern);

         log.connectingToMongo(this.cfg.getHost(), this.cfg.getPort(), this.cfg.getTimeout(), this.cfg.getAcknowledgment());

         ServerAddress serverAddress = new ServerAddress(this.cfg.getHost(), this.cfg.getPort());

         this.mongo = new MongoClient(serverAddress, optionBuilder.build());
      } catch (UnknownHostException e) {
         throw log.mongoOnUnknownHost(this.cfg.getHost());
      } catch (RuntimeException e) {
         throw log.unableToInitializeMongoDB(e);
      }
      mongoDb = extractDatabase();
      this.collection = mongoDb.getCollection(this.cfg.getCollectionName());

   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();
      log.disconnectingFromMongo();
      this.mongo.close();
   }

   private DB extractDatabase() throws CacheLoaderException {
      try {
         log.connectingToMongoDatabase(this.cfg.getDatabase());
         if (!"".equals(this.cfg.getUsername())) {
            DB admin = this.mongo.getDB("admin");
            boolean auth = admin.authenticate(this.cfg.getUsername(), this.cfg.getPassword().toCharArray());
            if (!auth) {
               throw log.authenticationFailed(this.cfg.getUsername());
            }
         }
         if ("".equals(this.cfg.getDatabase())) {
            throw log.mongoDbNameMissing();
         }
         if (!this.mongo.getDatabaseNames().contains(this.cfg.getDatabase())) {
            log.creatingDatabase(this.cfg.getDatabase());
         }
         return this.mongo.getDB(this.cfg.getDatabase());
      } catch (MongoException e) {
         throw log.unableToConnectToDatastore(this.cfg.getHost(), this.cfg.getPort(), e);
      }
   }
}
