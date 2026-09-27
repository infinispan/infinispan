package org.infinispan.client.hotrod.impl;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCacheManagerAdmin;
import org.infinispan.client.hotrod.RemoteSchemasAdmin;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.operations.ManagerOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.TimeoutHotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.commons.configuration.BasicConfiguration;

/**
 * @author Tristan Tarrant
 * @since 9.1
 */
public class RemoteCacheManagerAdminImpl implements RemoteCacheManagerAdmin {
   public static final String CACHE_NAME = "name";
   public static final String ALIAS_NAME = "alias";
   public static final String CACHE_TEMPLATE = "template";
   public static final String CACHE_CONFIGURATION = "configuration";
   public static final String ATTRIBUTE = "attribute";
   public static final String VALUE = "value";
   public static final String FLAGS = "flags";
   private final RemoteCacheManager cacheManager;
   private final ManagerOperationsFactory operationsFactory;
   private final OperationDispatcher operationDispatcher;
   private final EnumSet<AdminFlag> flags;
   private final Consumer<String> remover;
   /**
    * Timeout override in milliseconds set through {@link #withTimeout(long, TimeUnit)}. {@code -1} means the
    * configured {@link org.infinispan.client.hotrod.configuration.Configuration#longRunningOperationTimeout()} is used.
    */
   private final long timeout;

   public RemoteCacheManagerAdminImpl(RemoteCacheManager cacheManager, ManagerOperationsFactory operationsFactory,
                                      OperationDispatcher operationDispatcher, EnumSet<AdminFlag> flags, Consumer<String> remover) {
      this(cacheManager, operationsFactory, operationDispatcher, flags, remover, -1);
   }

   private RemoteCacheManagerAdminImpl(RemoteCacheManager cacheManager, ManagerOperationsFactory operationsFactory,
                                       OperationDispatcher operationDispatcher, EnumSet<AdminFlag> flags, Consumer<String> remover,
                                       long timeout) {
      this.cacheManager = cacheManager;
      this.operationsFactory = operationsFactory;
      this.operationDispatcher = operationDispatcher;
      this.flags = flags;
      this.remover = remover;
      this.timeout = timeout;
   }

   /**
    * Administrative operations may involve the whole cluster and the whole data set, so they are executed with the
    * long running operation timeout rather than with the socket timeout.
    */
   private String execute(String operationName, Map<String, byte[]> params) {
      long timeoutMillis = timeout > 0 ? timeout : cacheManager.getConfiguration().longRunningOperationTimeout();
      HotRodOperation<String> op = new TimeoutHotRodOperation<>(operationsFactory.executeOperation(operationName, params), timeoutMillis);
      return operationDispatcher.await(operationDispatcher.execute(op), timeoutMillis);
   }

   @Override
   public <K, V> RemoteCache<K, V> createCache(String name, String template) throws HotRodClientException {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (template != null) params.put(CACHE_TEMPLATE, string(template));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      execute("@@cache@create", params);
      return cacheManager.getCache(name);
   }

   @Override
   public <K, V> RemoteCache<K, V> createCache(String name, DefaultTemplate template) throws HotRodClientException {
      return createCache(name, template.getConfiguration());
   }

   @Override
   public <K, V> RemoteCache<K, V> createCache(String name, BasicConfiguration configuration) throws HotRodClientException {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (configuration != null) params.put(CACHE_CONFIGURATION, string(configuration.toStringConfiguration(name)));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      execute("@@cache@create", params);
      return cacheManager.getCache(name);
   }

   @Override
   public <K, V> RemoteCache<K, V> getOrCreateCache(String name, String template) throws HotRodClientException {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (template != null) params.put(CACHE_TEMPLATE, string(template));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      execute("@@cache@getorcreate", params);
      return cacheManager.getCache(name);
   }

   @Override
   public <K, V> RemoteCache<K, V> getOrCreateCache(String name, DefaultTemplate template) throws HotRodClientException {
      return getOrCreateCache(name, template.getConfiguration());
   }

   @Override
   public <K, V> RemoteCache<K, V> getOrCreateCache(String name, BasicConfiguration configuration) throws HotRodClientException {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (configuration != null) params.put(CACHE_CONFIGURATION, string(configuration.toStringConfiguration(name)));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      execute("@@cache@getorcreate", params);
      return cacheManager.getCache(name);
   }

   @Override
   public void removeCache(String name) {
      remover.accept(name);
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      execute("@@cache@remove", params);
   }

   @Override
   public RemoteCacheManagerAdmin withFlags(AdminFlag... flags) {
      EnumSet<AdminFlag> newFlags = EnumSet.copyOf(this.flags);
      Collections.addAll(newFlags, flags);
      return new RemoteCacheManagerAdminImpl(cacheManager, operationsFactory, operationDispatcher, newFlags, remover, timeout);
   }

   @Override
   public RemoteCacheManagerAdmin withFlags(EnumSet<AdminFlag> flags) {
      EnumSet<AdminFlag> newFlags = EnumSet.copyOf(this.flags);
      newFlags.addAll(flags);
      return new RemoteCacheManagerAdminImpl(cacheManager, operationsFactory, operationDispatcher, newFlags, remover, timeout);
   }

   @Override
   public void reindexCache(String name) throws HotRodClientException {
      execute("@@cache@reindex", Collections.singletonMap(CACHE_NAME, string(name)));
   }

   @Override
   public void reindexCache(String name, long timeout, TimeUnit timeUnit) throws HotRodClientException {
      withTimeout(timeout, timeUnit).reindexCache(name);
   }

   @Override
   public void updateIndexSchema(String name) throws HotRodClientException {
      execute("@@cache@updateindexschema", Collections.singletonMap(CACHE_NAME, string(name)));
   }

   @Override
   public void updateIndexSchema(String name, long timeout, TimeUnit timeUnit) throws HotRodClientException {
      withTimeout(timeout, timeUnit).updateIndexSchema(name);
   }

   @Override
   public void updateConfigurationAttribute(String name, String attribute, String value) throws HotRodClientException {
      Map<String, byte[]> params = new HashMap<>(4);
      params.put(CACHE_NAME, string(name));
      params.put(ATTRIBUTE, string(attribute));
      params.put(VALUE, string(value));

      if (flags != null && !flags.isEmpty()) {
         params.put(FLAGS, flags(flags));
      }

      execute("@@cache@updateConfigurationAttribute", params);
   }

   @Override
   public void createTemplate(String name, BasicConfiguration configuration) {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (configuration != null) params.put(CACHE_CONFIGURATION, string(configuration.toStringConfiguration(name)));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      execute("@@template@create", params);
   }

   @Override
   public void removeTemplate(String name) {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      execute("@@template@remove", params);
   }

   @Override
   public void assignAlias(String aliasName, String cacheName) throws HotRodClientException {
      Map<String, byte[]> params = new HashMap<>(4);
      params.put(CACHE_NAME, string(cacheName));
      params.put(ALIAS_NAME, string(aliasName));
      if (flags != null && !flags.isEmpty()) {
         params.put(FLAGS, flags(flags));
      }
      execute("@@cache@assignAlias", params);
   }

   @Override
   public RemoteCacheManagerAdmin withTimeout(long timeout, TimeUnit timeUnit) {
      long timeoutMillis = Objects.requireNonNull(timeUnit, "TimeUnit must not be null").toMillis(timeout);
      if (timeoutMillis <= 0) {
         throw HOTROD.invalidOperationTimeout(timeoutMillis);
      }
      return new RemoteCacheManagerAdminImpl(cacheManager, operationsFactory, operationDispatcher, flags, remover, timeoutMillis);
   }

   @Override
   public RemoteSchemasAdmin schemas() {
      return new RemoteSchemasAdminImpl(operationsFactory, operationDispatcher, cacheManager);
   }

   private static byte[] flags(EnumSet<AdminFlag> flags) {
      String sFlags = flags.stream().map(AdminFlag::toString).collect(Collectors.joining(","));
      return string(sFlags);
   }

   protected static byte[] string(String s) {
      return s.getBytes(HotRodConstants.HOTROD_STRING_CHARSET);
   }
}
