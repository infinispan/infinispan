package org.infinispan.server.resp.scripting;

import java.util.Collection;
import java.util.Iterator;

import org.infinispan.commons.jdkspecific.CallerId;

import party.iroiro.luajava.Lua;

/**
 * A read-only collection implementation that maps lua tables. Only size and iterations are supported.
 */
public class LuaCollection<V> implements Collection<V> {
   private final Lua lua;
   private final int size;
   private final int index;

   public LuaCollection(Lua lua, int index) {
      this(lua, index, lua.rawLength(index));
   }

   protected LuaCollection(Lua lua, int index, int size) {
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
   public boolean contains(Object o) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public Iterator<V> iterator() {
      return new LuaIterator();
   }

   @Override
   public Object[] toArray() {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public boolean add(Object o) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public boolean remove(Object o) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public boolean addAll(Collection<? extends V> c) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }


   @Override
   public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public void clear() {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   private class LuaIterator implements Iterator<V> {

      private LuaIterator() {
         // We start iterating by pushing a 'nil'
         lua.pushNil();
      }

      @Override
      public boolean hasNext() {
         return lua.next(index) != 0;
      }

      @Override
      public V next() {
         return null;
      }
   }
}
