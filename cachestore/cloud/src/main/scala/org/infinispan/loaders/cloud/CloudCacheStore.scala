package org.infinispan.loaders.cloud

import org.infinispan.util.logging.LogFactory
import org.infinispan.Cache
import org.infinispan.marshall.Marshaller
import org.infinispan.config.ConfigurationException
import org.infinispan.util.Util
import org.jclouds.http.HttpUtils
import java.util.{HashSet, Properties}
import org.infinispan.container.entries.InternalCacheEntry
import org.infinispan.loaders.bucket.{Bucket, BucketBasedCacheStore}
import org.infinispan.loaders.modifications.Modification
import java.util.concurrent.{ExecutionException, Future}
import org.jclouds.blobstore.domain.Blob
import java.io.{IOException, ObjectOutput, ObjectInput}
import scala.collection._
import reflect.BeanProperty
import org.infinispan.loaders.{LockSupportCacheStoreConfig, CacheLoaderConfig, CacheLoaderException}
import org.jclouds.blobstore._
import org.scala_tools.javautils.Imports._
import org.jclouds.concurrent.config.ExecutorServiceModule

/**
 * The CloudCacheStore implementation that utilizes <a href="http://code.google.com/p/jclouds">JClouds</a> to communicate
 * with cloud storage providers such as <a href="http://aws.amazon.com/s3/">Amazon's S3<a>,
 * <a href="http://www.rackspacecloud.com/cloud_hosting_products/files">Rackspace's Cloudfiles</a>, or any other such
 * provider supported by JClouds.
 * <p />
 * This file store stores stuff in the following format:
 * <tt>http://    { cloud-storage-provider } /    { bucket } /    { bucket_number }.bucket</tt>
 * <p /> 
 * @author Manik Surtani
 * @since 4.0
 */

class CloudCacheStore extends BucketBasedCacheStore {
   val log = LogFactory.getLog(classOf[CloudCacheStore])
   val asyncCommandFutures = new ThreadLocal[mutable.Set[Future[Any]]]()
   var cfg: CloudCacheStoreConfig = null
   var containerName: String = null
   var ctx: BlobStoreContext[Any, Any] = null
   var blobStore: BlobStore = null
   var asyncBlobStore: AsyncBlobStore = null
   var pollFutures = false

   override def getConfigurationClass() = classOf[CloudCacheStoreConfig]

   def getThisContainerName() = cfg.bucketPrefix + "-" + cache.getName.toLowerCase

   override def supportsMultiThreadedPurge() = true

   override def init(cfg: CacheLoaderConfig, cache: Cache[_, _], m: Marshaller) {
      this.cfg = cfg.asInstanceOf[CloudCacheStoreConfig]
      init(cfg, cache, m, null, null, null)
   }

   def init(cfg: CacheLoaderConfig, cache: Cache[_, _], m: Marshaller, ctx: BlobStoreContext[Any, Any], blobStore: BlobStore, asyncBlobStore: AsyncBlobStore) {
      super.init(cfg, cache, m)
      this.cfg = cfg.asInstanceOf[CloudCacheStoreConfig]
      this.cache = cache
      this.marshaller = m
      this.ctx = ctx
      this.blobStore = blobStore
      this.asyncBlobStore = asyncBlobStore
   }

   override def start() {
      super.start
      if (cfg.cloudService == null) throw new ConfigurationException("CloudService must be set!")
      if (cfg.identity == null) throw new ConfigurationException("Identity must be set")
      if (cfg.password == null) throw new ConfigurationException("Password must be set")
      if (cfg.bucketPrefix == null) throw new ConfigurationException("CloudBucket must be set")
      containerName = getThisContainerName
      ctx = new BlobStoreContextFactory().createContext(cfg.cloudService, cfg.identity, cfg.password).asInstanceOf[BlobStoreContext[Any, Any]]
      val bs = ctx.getBlobStore
      // the "location" is not currently used.
      if (!bs.containerExists(containerName)) bs.createContainerInLocation(null, containerName)
      blobStore = ctx.getBlobStore
      asyncBlobStore = ctx.getAsyncBlobStore
      pollFutures = !(cfg.getAsyncStoreConfig.isEnabled.booleanValue)
   }

   def loadAllLockSafe() = {
      val result = new HashSet[InternalCacheEntry]()
//      val values = ctx.createBlobMap(containerName).values
      val entries = ctx.createBlobMap(containerName).entrySet
      for (entry <- entries) {
         val bucket = readFromBlob(entry.getValue, entry.getKey)
         if (bucket.removeExpiredEntries) updateBucket(bucket)
         result addAll bucket.getStoredEntries
      }
      result
   }

   def fromStreamLockSafe(objectInput: ObjectInput) {
      var source: String = null
      try {
         source = objectInput.readObject.asInstanceOf[String]
      } catch {
         case e => throw convertToCacheLoaderException("Error while reading from stream", e)
      }
      if (getThisContainerName equals source) {
         log.info("Attempt to load the same cloud bucket ({0}) ignored", source)
      } else {
         // TODO implement stream handling.   What's the JClouds API to "copy" one bucket to another?
      }
   }

   def toStreamLockSafe(objectOutput: ObjectOutput) {
      try {
         objectOutput writeObject getThisContainerName
      } catch {
         case e => throw convertToCacheLoaderException("Error while writing to stream", e)
      }
   }

   def clearLockSafe() {
      val futures: mutable.Set[Future[Any]] = asyncCommandFutures.get
      if (futures == null) {
         // is a sync call
         blobStore clearContainer containerName
      } else {
         // is an async call - invoke clear() on the container asynchronously and store the future in the 'futures' collection
         futures += (asyncBlobStore clearContainer containerName).asInstanceOf[Future[Any]]
      }
   }

   def convertToCacheLoaderException(m: String, c: Throwable) = {
      if (c.isInstanceOf[CacheLoaderException]) {
         c.asInstanceOf[CacheLoaderException]
      } else {
         new CacheLoaderException(m, c)
      }
   }

   def loadBucket(hash: String) = {
      try {
         readFromBlob(blobStore.getBlob(containerName, hash), hash)
      } catch {
         case e: KeyNotFoundException => null
      }
   }

   def purge(blobMap: BlobMap) {
      for (entry <- blobMap.entrySet) {
         val bucket = readFromBlob(entry.getValue, entry.getKey)
         if (bucket.removeExpiredEntries) updateBucket(bucket)
      }
   }

   def purgeInternal() {
      // TODO can expiry data be stored in a blob's metadata?  More efficient purging that way.  See https://jira.jboss.org/jira/browse/ISPN-334
      if (!cfg.lazyPurgingOnly) {
         acquireGlobalLock(false)
         try {
            val blobMap = ctx createBlobMap containerName
            if (multiThreadedPurge) {
               purgerService.execute(new Runnable() {
                  def run() {
                     try {
                        purge(blobMap)
                     } catch {
                        case e => log.warn("Problems purging", e)
                     }
                  }
               })
            } else {
               purge(blobMap)
            }
         } finally {
            releaseGlobalLock(false)
         }
      }
   }

   def insertBucket(bucket: Bucket) {
      val blob = blobStore.newBlob(getBucketName(bucket))
      writeToBlob(blob, bucket)

      val futures: mutable.Set[Future[Any]] = asyncCommandFutures.get();
      if (futures == null) {
         // is a sync call
         blobStore.putBlob(containerName, blob)
      } else {
         // is an async call - invoke clear() on the container asynchronously and store the future in the 'futures' collection
         futures += (asyncBlobStore.putBlob(containerName, blob)).asInstanceOf[Future[Any]]
      }
   }

   override def applyModifications(mods: java.util.List[_ <: Modification]) {
      val futures = mutable.Set[Future[Any]]()
      asyncCommandFutures set futures
      try {
         super.applyModifications(mods)
         if (pollFutures) {
            var exception: CacheLoaderException = null
            try {
               for (f <- asyncCommandFutures.get) f.get
            } catch {
               case ie: InterruptedException => Thread.currentThread.interrupt
               case ee: ExecutionException => {
                  if (ee.getCause.isInstanceOf[CacheLoaderException])
                     exception = ee.getCause.asInstanceOf[CacheLoaderException]
                  else
                     exception = new CacheLoaderException(ee.getCause)
               }
            }
            if (exception != null) throw exception
         }
      } finally {
         asyncCommandFutures.remove
      }
   }

   def updateBucket(bucket: Bucket) {insertBucket(bucket)}

   def writeToBlob(blob: Blob, bucket: Bucket) {
      try {
         blob setPayload marshaller.objectToByteBuffer(bucket)
      } catch {
         case e: IOException => throw new CacheLoaderException(e)
      }
   }

   def readFromBlob(blob: Blob, bucketName: String): Bucket = {
      if (blob == null) return null
      try {
         val bucket = marshaller.objectFromInputStream(blob.getContent).asInstanceOf[Bucket]
         if (bucket != null) bucket setBucketName bucketName
         bucket
      } catch {
         case e => throw new CacheLoaderException(e)
      }
   }

   def getBucketName(bucket: Bucket) = {
      log.warn("Bucket is {0}", bucket)
      val bucketName = bucket.getBucketName
      if (bucketName startsWith "-")
         bucketName replace ("-", "A")
      else
         bucketName
   }
}

/**
 * The cache store config bean for this cache store implementation
 * @author Manik Surtani
 * @since 4.0
 */
// much more concise and expressive than a Java counterpart!
class CloudCacheStoreConfig(
      @BeanProperty var identity: String,
      @BeanProperty var password: String,
      @BeanProperty var bucketPrefix: String,
      @BeanProperty var proxyHost: String,
      @BeanProperty var proxyPort: String,
      @BeanProperty var requestTimeout: Long,
      @BeanProperty var lazyPurgingOnly: Boolean,
      @BeanProperty var cloudService: String,
      @BeanProperty var maxConnections: Int,
      @BeanProperty var secure: Boolean
      ) extends LockSupportCacheStoreConfig {
   setCacheLoaderClassName(classOf[CloudCacheStore].getName)

   def this() = this (null, null, null, null, null, 10000, true, null, 3, true)
}

class CloudConnectionException(m: String, c: Throwable) extends CacheLoaderException(m, c) {
   def this() = this ("", null)

   def this(m: String) = this (m, null)

   def this(c: Throwable) = this ("", c)
}