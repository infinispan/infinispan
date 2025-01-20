package org.infinispan.server.resp.scripting;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.infinispan.commons.jdkspecific.CallerId;

import party.iroiro.luajava.Lua;

/**
 * A read-only set implementation that maps lua tables. Only size and iterations are supported.
 */
public class LuaSet<V> implements Set<V> {
   private static final Object ITEM = new Object();
   private final Lua lua;
   private final int index;
   private final int size;
   private final V item;

   public LuaSet(Lua lua, int index, int size) {
      this(lua, index, size, (V) ITEM);
   }

   public LuaSet(Lua lua, int index, int size, V item) {
      this.lua = lua;
      this.index = index;
      this.size = size;
      this.item = item;
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
   public boolean contains(Object o) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public Iterator<V> iterator() {
      return new LuaSetIterator();
   }

   @Override
   public Object[] toArray() {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public boolean add(V v) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public boolean addAll(Collection<? extends V> c) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   private class LuaSetIterator implements Iterator<V> {
      private LuaSetIterator() {
         lua.pushNil();
      }

      @Override
      public boolean hasNext() {
         return lua.next(index) != 0;
      }

      @Override
      public V next() {
         return item;
      }
   }
}
