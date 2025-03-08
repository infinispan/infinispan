package org.infinispan.server.resp.scripting;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.jdkspecific.CallerId;
import org.infinispan.commons.util.SimpleImmutableEntry;

import party.iroiro.luajava.Lua;

/**
 * A read-only map implementation that maps lua tables. Only size and iterations are supported.
 */
public class LuaMap<K, V> implements Map<K, V> {
   static final Entry<Object, Object> EMPTY_ENTRY = new SimpleImmutableEntry<>(new Object(), new Object());
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
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public boolean containsValue(Object value) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public V get(Object key) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public V put(Object key, Object value) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public V remove(Object key) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }


   @Override
   public Set<K> keySet() {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }


   @Override
   public Collection<V> values() {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      return new LuaSet<>(lua, index, size, (Entry<K, V>) EMPTY_ENTRY);
   }

}
