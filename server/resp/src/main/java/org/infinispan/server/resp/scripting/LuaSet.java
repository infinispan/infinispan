package org.infinispan.server.resp.scripting;

import java.util.Set;

import party.iroiro.luajava.Lua;

public class LuaSet<V> extends LuaCollection<V> implements Set<V> {

   public LuaSet(Lua lua, int index, int size) {
      super(lua, index, size);
   }
}
