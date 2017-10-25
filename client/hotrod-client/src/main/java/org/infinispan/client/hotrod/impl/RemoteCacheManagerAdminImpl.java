package org.infinispan.client.hotrod.impl;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCacheManagerAdmin;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;

/**
 * @author Tristan Tarrant
 * @since 9.1
 */

public class RemoteCacheManagerAdminImpl implements RemoteCacheManagerAdmin {
   public static final String CACHE_NAME = "name";
   public static final String CACHE_TEMPLATE = "template";
   public static final String FLAGS = "flags";
   private final OperationsFactory operationsFactory;
   private final EnumSet<AdminFlag> flags;

   public RemoteCacheManagerAdminImpl(OperationsFactory operationsFactory, EnumSet<AdminFlag> flags) {
      this.operationsFactory = operationsFactory;
      this.flags = flags;
   }

   @Override
   public void createCache(String name, String template) {
      Map<String, byte[]> params = new HashMap<>(2);
      params.put(CACHE_NAME, string(name));
      if (template != null) params.put(CACHE_TEMPLATE, string(template));
      if (flags != null && !flags.isEmpty()) params.put(FLAGS, flags(flags));
      operationsFactory.newExecuteOperation("@@cache@create", params).execute();
   }

   @Override
   public void createCache(String name, String template, EnumSet<org.infinispan.client.hotrod.AdminFlag> flags) {
      EnumSet<AdminFlag> newFlags = EnumSet.noneOf(AdminFlag.class);
      for(org.infinispan.client.hotrod.AdminFlag flag : flags) newFlags.add(flag.upgrade());
      new RemoteCacheManagerAdminImpl(operationsFactory, newFlags).createCache(name, template);
   }

   @Override
   public void removeCache(String name) {
      operationsFactory.newExecuteOperation("@@cache@remove", Collections.singletonMap(CACHE_NAME, string(name))).execute();
   }

   @Override
   public RemoteCacheManagerAdmin withFlags(AdminFlag... flags) {
      EnumSet<AdminFlag> newFlags = EnumSet.copyOf(this.flags);
      for(AdminFlag flag : flags)
         newFlags.add(flag);
      return new RemoteCacheManagerAdminImpl(operationsFactory, newFlags);
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
