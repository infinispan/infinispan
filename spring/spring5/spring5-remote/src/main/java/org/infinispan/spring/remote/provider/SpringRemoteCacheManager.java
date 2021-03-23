package org.infinispan.spring.remote.provider;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.MarshallerRegistry;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.spring.common.provider.NullValue;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.MapSessionProtoAdapter;
import org.springframework.session.MapSession;
import org.springframework.util.Assert;

/**
 * <p>
 * A {@link org.springframework.cache.CacheManager <code>CacheManager</code>} implementation that is
 * backed by an {@link RemoteCacheManager
 * <code>Infinispan RemoteCacheManager</code>} instance.
 * </p>
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @author Marius Bogoevici
 *
 */
public class SpringRemoteCacheManager implements org.springframework.cache.CacheManager {

   private final RemoteCacheManager nativeCacheManager;
   private final ConcurrentMap<String, SpringCache> springCaches = new ConcurrentHashMap<>();
   private volatile long readTimeout;
   private volatile long writeTimeout;

   /**
    * @param nativeCacheManager the underlying cache manager
    */
   public SpringRemoteCacheManager(final RemoteCacheManager nativeCacheManager, long readTimeout, long writeTimeout) {
      Assert.notNull(nativeCacheManager,
                     "A non-null instance of EmbeddedCacheManager needs to be supplied");
      this.nativeCacheManager = nativeCacheManager;
      this.readTimeout = readTimeout;
      this.writeTimeout = writeTimeout;

      configureMarshallers(nativeCacheManager);
   }

   public SpringRemoteCacheManager(final RemoteCacheManager nativeCacheManager) {
      this(nativeCacheManager, 0, 0);
   }

   /**
    * @see org.springframework.cache.CacheManager#getCache(String)
    */
   @Override
   public SpringCache getCache(final String name) {
      final RemoteCache<Object, Object> nativeCache = this.nativeCacheManager.getCache(name);
      if (nativeCache == null) {
         springCaches.remove(name);
         return null;
      }

      return springCaches.computeIfAbsent(name, n -> new SpringCache(nativeCache, readTimeout, writeTimeout));
   }

   /**
    * @see org.springframework.cache.CacheManager#getCacheNames()
    */
   @Override
   public Collection<String> getCacheNames() {
      return this.nativeCacheManager.getCacheNames();
   }

   /**
    * Return the {@link RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    *
    * @return The {@link RemoteCacheManager
    *         <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    *         <code>SpringRemoteCacheManager</code>
    */
   public RemoteCacheManager getNativeCacheManager() {
      return this.nativeCacheManager;
   }

   public long getReadTimeout() {
      return this.readTimeout;
   }

   public long getWriteTimeout() {
      return this.writeTimeout;
   }

   public void setReadTimeout(final long readTimeout) {
      this.readTimeout = readTimeout;
   }

   public void setWriteTimeout(final long writeTimeout) {
      this.writeTimeout = writeTimeout;
   }

   /**
    * Start the {@link RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    */
   public void start() {
      this.nativeCacheManager.start();
   }

   /**
    * Stop the {@link RemoteCacheManager
    * <code>org.infinispan.client.hotrod.RemoteCacheManager</code>} that backs this
    * <code>SpringRemoteCacheManager</code>.
    */
   public void stop() {
      this.nativeCacheManager.stop();
      this.springCaches.clear();
   }

   private void configureMarshallers(RemoteCacheManager nativeCacheManager) {
      MarshallerRegistry marshallerRegistry = nativeCacheManager.getMarshallerRegistry();

      // Java serialization support
      JavaSerializationMarshaller serializationMarshaller =
            (JavaSerializationMarshaller) marshallerRegistry.getMarshaller(MediaType.APPLICATION_SERIALIZED_OBJECT);
      if (serializationMarshaller == null) {
         // Register a JavaSerializationMarshaller if it doesn't exist yet
         // Because some session attributes are always marshalled with Java serialization
         serializationMarshaller = new JavaSerializationMarshaller();
         marshallerRegistry.registerMarshaller(serializationMarshaller);
      }

      // Extend deserialization allow list
      ClassAllowList serializationAllowList = new ClassAllowList();
      serializationAllowList.addClasses(NullValue.class);
      serializationAllowList.addRegexps("java.util\\..*", "org.springframework\\..*");
      serializationMarshaller.initialize(serializationAllowList);

      // Protostream support
      ProtoStreamMarshaller protoMarshaller =
            (ProtoStreamMarshaller) marshallerRegistry.getMarshaller(MediaType.APPLICATION_PROTOSTREAM);
      if (protoMarshaller == null) {
         try {
            protoMarshaller = new ProtoStreamMarshaller();
            marshallerRegistry.registerMarshaller(protoMarshaller);

            // Apply the serialization context initializers in the configuration first
            SerializationContext ctx = protoMarshaller.getSerializationContext();
            for (SerializationContextInitializer sci : nativeCacheManager.getConfiguration().getContextInitializers()) {
               sci.registerSchema(ctx);
               sci.registerMarshallers(ctx);
            }
         } catch (NoClassDefFoundError e) {
            // Ignore the error, the protostream dependency is missing
         }
      }
      if (protoMarshaller != null) {
         // Apply our own serialization context initializers
         SerializationContext ctx = protoMarshaller.getSerializationContext();
         addProviderContextInitializer(ctx);
         addSessionContextInitializerAndMarshaller(ctx, serializationMarshaller);
      }
   }

   private void addProviderContextInitializer(SerializationContext ctx) {
      org.infinispan.spring.common.provider.PersistenceContextInitializerImpl providerSci =
            new org.infinispan.spring.common.provider.PersistenceContextInitializerImpl();
      providerSci.registerMarshallers(ctx);
      providerSci.registerSchema(ctx);
   }

   private void addSessionContextInitializerAndMarshaller(SerializationContext ctx,
                                                          JavaSerializationMarshaller serializationMarshaller) {
      // Skip registering the marshallers if the MapSession class is not available
      try {
         new MapSession();
      } catch (NoClassDefFoundError e) {
         Log.CONFIG.debug("spring-session classes not found, skipping the session context initializer registration");
         return;
      }

      org.infinispan.spring.common.session.PersistenceContextInitializerImpl sessionSci =
            new org.infinispan.spring.common.session.PersistenceContextInitializerImpl();
      sessionSci.registerMarshallers(ctx);
      sessionSci.registerSchema(ctx);

      BaseMarshaller sessionAttributeMarshaller =
            new MapSessionProtoAdapter.SessionAttributeRawMarshaller(serializationMarshaller);
      ctx.registerMarshaller(sessionAttributeMarshaller);
   }
}
