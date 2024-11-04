package org.infinispan.server.resp.scripting;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.jdkspecific.CallerId;

import party.iroiro.luajava.Lua;

public class LuaMap<K, V> implements Map<K, V> {
   private final Lua lua;
   private final int index;
   private final int size;

   public LuaMap(Lua lua, int index, int size) {
      this.lua = lua;
      this.index = index;
      this.size = size;
   }

   @Override
   public int size() {
      return size;
   }

   @Override
   public boolean isEmpty() {
      return size == 0;
   }

   @Override
   public boolean containsKey(Object key) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public boolean containsValue(Object value) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public V get(Object key) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public V put(Object key, Object value) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public V remove(Object key) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }


   @Override
   public Set<K> keySet() {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }


   @Override
   public Collection<V> values() {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      return new LuaSet<>(lua, index, size);
   }
}
