package org.infinispan.client.hotrod.impl;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCacheManagerAdmin;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.commons.configuration.BasicConfiguration;

/**
 * @author Tristan Tarrant
 * @since 9.1
 */
public class RemoteCacheManagerAdminImpl implements RemoteCacheManagerAdmin {
   public static final String CACHE_NAME = "name";
   public static final String CACHE_TEMPLATE = "template";
   public static final String CACHE_CONFIGURATION = "configuration";
   public static final String FLAGS = "flags";
   private final RemoteCacheManager cacheManager;
   private final OperationsFactory operationsFactory;
   private final EnumSet<AdminFlag> flags;
   private final Consumer<String> remover;

   public RemoteCacheManagerAdminImpl(RemoteCacheManager cacheManager, OperationsFactory operationsFactory, EnumSet<AdminFlag> flags, Consumer<String> remover) {
      this.cacheManager = cacheManager;
      this.operationsFactory = operationsFactory;
      this.flags = flags;
      this.remover = remover;
   }

   @Override
   public <K, V> RemoteCache<K, V> createCache(String name, String template) throws HotRodClientException {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (template != null) params.put(CACHE_TEMPLATE, string(template));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      operationsFactory.newExecuteOperation("@@cache@create", params).execute();
      return cacheManager.getCache(name);
   }

   @Override
   public <K, V> BasicCache<K, V> createCache(String name, BasicConfiguration configuration) throws HotRodClientException {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (configuration != null) params.put(CACHE_CONFIGURATION, string(configuration.toXMLString()));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      operationsFactory.newExecuteOperation("@@cache@create", params).execute();
      return cacheManager.getCache(name);
   }

   @Override
   public <K, V> RemoteCache<K, V> getOrCreateCache(String name, String template) throws HotRodClientException {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (template != null) params.put(CACHE_TEMPLATE, string(template));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      operationsFactory.newExecuteOperation("@@cache@getorcreate", params).execute();
      return cacheManager.getCache(name);
   }

   @Override
   public <K, V> BasicCache<K, V> getOrCreateCache(String name, BasicConfiguration configuration) throws HotRodClientException {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (configuration != null) params.put(CACHE_CONFIGURATION, string(configuration.toXMLString()));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      operationsFactory.newExecuteOperation("@@cache@getorcreate", params).execute();
      return cacheManager.getCache(name);
   }

   @Override
   public void createCache(String name, String template, EnumSet<org.infinispan.client.hotrod.AdminFlag> flags) {
      EnumSet<AdminFlag> newFlags = EnumSet.noneOf(AdminFlag.class);
      for(org.infinispan.client.hotrod.AdminFlag flag : flags) newFlags.add(flag.upgrade());
      new RemoteCacheManagerAdminImpl(cacheManager, operationsFactory, newFlags, remover).createCache(name, template);
   }

   @Override
   public void removeCache(String name) {
      remover.accept(name);
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      operationsFactory.newExecuteOperation("@@cache@remove", params).execute();
   }

   @Override
   public RemoteCacheManagerAdmin withFlags(AdminFlag... flags) {
      EnumSet<AdminFlag> newFlags = EnumSet.copyOf(this.flags);
      for(AdminFlag flag : flags)
         newFlags.add(flag);
      return new RemoteCacheManagerAdminImpl(cacheManager, operationsFactory, newFlags, remover);
   }

   @Override
   public RemoteCacheManagerAdmin withFlags(EnumSet<AdminFlag> flags) {
      EnumSet<AdminFlag> newFlags = EnumSet.copyOf(this.flags);
      newFlags.addAll(flags);
      return new RemoteCacheManagerAdminImpl(cacheManager, operationsFactory, newFlags, remover);
   }

   @Override
   public void reindexCache(String name) throws HotRodClientException {
      operationsFactory.newExecuteOperation("@@cache@reindex", Collections.singletonMap(CACHE_NAME, string(name))).execute();
   }

   private static byte[] flags(EnumSet<AdminFlag> flags) {
      String sFlags = flags.stream().map(AdminFlag::toString).collect(Collectors.joining(","));
      return string(sFlags);
   }

   private static byte[] string(String s) {
      return s.getBytes(HotRodConstants.HOTROD_STRING_CHARSET);
   }
}
