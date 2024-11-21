package org.infinispan.client.hotrod;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

/**
 * StreamingRemoteCache implements streaming versions of most {@link RemoteCache} methods
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public interface StreamingRemoteCache<K> {
   /**
    * Retrieves the value of the specified key as an {@link InputStream}. It is up to the application to ensure
    * that the stream is consumed and closed. The marshaller is ignored, i.e. all data will be read in its
    * raw binary form. The returned input stream implements the {@link VersionedMetadata} interface.
    * The returned input stream is not thread-safe.
    *
    * @param key      key to use
    */
   <T extends InputStream & VersionedMetadata> T get(K key);

   /**
    * Initiates a streaming put operation. It is up to the application to write to the returned {@link OutputStream}
    * and close it when there is no more data to write. The marshaller is ignored, i.e. all data will be written in its
    * raw binary form. The returned output stream is not thread-safe.
    *
    * @param key      key to use
    */
   OutputStream put(K key);

   /**
    * An overloaded form of {@link #put(Object)}, which takes in lifespan parameters.
    * The returned output stream is not thread-safe.
    *
    * @param key      key to use
    * @param lifespan lifespan of the entry.  Negative values are interpreted as unlimited lifespan.
    * @param lifespanUnit     unit of measurement for the lifespan
    */
   OutputStream put(K key, long lifespan, TimeUnit lifespanUnit);

   /**
    * An overloaded form of {@link #put(Object)}, which takes in lifespan and maxIdle parameters.
    * The returned output stream is not thread-safe.
    *
    * @param key      key to use
    * @param lifespan lifespan of the entry
    * @param lifespanUnit {@link java.util.concurrent.TimeUnit} for lifespan
    * @param maxIdle the maximum amount of time this key is allowed
    *                           to be idle for before it is considered as expired
    * @param maxIdleUnit {@link java.util.concurrent.TimeUnit} for maxIdle
    */
   OutputStream put(K key, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * A conditional form of put which inserts an entry into the cache only if no mapping for the key is already present.
    * The operation is atomic. The server only performs the operation once the stream has been closed.
    * The returned output stream is not thread-safe.
    *
    * @param key     key to use
    */
   OutputStream putIfAbsent(K key);

   /**
    * An overloaded form of {@link #putIfAbsent(Object)} which takes in lifespan parameters.
    * The returned output stream is not thread-safe.
    *
    * @param key      key to use
    * @param lifespan lifespan of the entry
    * @param lifespanUnit {@link java.util.concurrent.TimeUnit} for lifespan
    */
   OutputStream putIfAbsent(K key, long lifespan, TimeUnit lifespanUnit);

   /**
    * An overloaded form of {@link #putIfAbsent(Object)} which takes in lifespan and maxIdle parameters.
    * The returned output stream is not thread-safe.
    *
    * @param key      key to use
    * @param lifespan lifespan of the entry
    * @param lifespanUnit {@link java.util.concurrent.TimeUnit} for lifespan
    * @param maxIdle the maximum amount of time this key is allowed
    *                           to be idle for before it is considered as expired
    * @param maxIdleUnit {@link java.util.concurrent.TimeUnit} for maxIdle
    */
   OutputStream putIfAbsent(K key, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);

   /**
    * A form of {@link #put(Object)}, which takes in a version. The value will be replaced on the server only if
    * the existing entry's version matches. The returned output stream is not thread-safe.
    *
    * @param key      key to use
    * @param version  the version to check for
    */
   OutputStream replaceWithVersion(K key, long version);

   /**
    * An overloaded form of {@link #replaceWithVersion(Object, long)} which takes in lifespan parameters.
    * The returned output stream is not thread-safe.
    *
    * @param key      key to use
    * @param version  the version to check for
    * @param lifespan lifespan of the entry
    * @param lifespanUnit {@link java.util.concurrent.TimeUnit} for lifespan
    */
   OutputStream replaceWithVersion(K key, long version, long lifespan, TimeUnit lifespanUnit);

   /**
    * An overloaded form of {@link #replaceWithVersion(Object, long)} which takes in lifespan and maxIdle parameters.
    * The returned output stream is not thread-safe.
    *
    * @param key      key to use
    * @param version  the version to check for
    * @param lifespan lifespan of the entry
    * @param lifespanUnit {@link java.util.concurrent.TimeUnit} for lifespan
    * @param maxIdle the maximum amount of time this key is allowed
    *                           to be idle for before it is considered as expired
    * @param maxIdleUnit {@link java.util.concurrent.TimeUnit} for maxIdle
    */
   OutputStream replaceWithVersion(K key, long version, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit);
}
