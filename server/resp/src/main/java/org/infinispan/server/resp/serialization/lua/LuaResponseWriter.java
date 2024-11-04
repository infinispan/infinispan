package org.infinispan.server.resp.serialization.lua;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.jdkspecific.CallerId;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;

import party.iroiro.luajava.Lua;

public class LuaResponseWriter implements ResponseWriter {
   private final Lua lua;

   public LuaResponseWriter(Lua lua) {
      this.lua = lua;
   }

   @Override
   public void nulls() {
      lua.checkStack(1);
      lua.pushNil();
   }

   @Override
   public void ok() {
      lua.checkStack(1);
      lua.push("OK");
   }

   @Override
   public void queued(Object ignore) {
      throw new UnsupportedOperationException("queued(Object)");
   }

   @Override
   public void simpleString(CharSequence value) {
      lua.checkStack(1);
      lua.push(value.toString());
   }

   @Override
   public void string(CharSequence value) {
      lua.checkStack(1);
      lua.push(value.toString());
   }

   @Override
   public void string(byte[] value) {
      lua.checkStack(1);
      lua.push(new String(value, StandardCharsets.US_ASCII));
   }

   @Override
   public void integers(Number value) {
      lua.checkStack(1);
      lua.push(value);
   }

   @Override
   public void doubles(Number value) {
      lua.checkStack(1);
      lua.push(value);
   }

   @Override
   public void booleans(boolean value) {
      lua.checkStack(1);
      lua.push(value);
   }

   @Override
   public void arrayEmpty() {
      lua.checkStack(1);
      lua.push(false);
   }

   @Override
   public <T> void array(Collection<T> collection, JavaObjectSerializer<T> serializer) {
      throw new UnsupportedOperationException("array(collection, serializer)");
   }

   @Override
   public void array(Collection<?> collection, Resp3Type contentType) {
      lua.checkStack(2);
      lua.newTable();
      int i = 1;
      for (Object o : collection) {
         lua.push(i);
         contentType.serialize(o, this);
         lua.setTable(-3);
         i++;
      }
   }

   @Override
   public void emptySet() {
      throw new UnsupportedOperationException("emptySet"); // Only used by COMMAND which cannot be called from Lua.
   }

   @Override
   public void set(Set<?> set, Resp3Type contentType) {
      /*
       * A set is represented as a lua table with two elements: 'set', containing a table
       * with the set entries as keys and a simple true boolean as value, and `len` containing the size of the set.
       */
      lua.checkStack(3);
      lua.newTable();
      lua.push("set");
      lua.newTable();
      for (Object o : set) {
         contentType.serialize(o, this);
         lua.checkStack(1);
         lua.push(true);
         lua.setTable(-3);
      }
      lua.setTable(-3);
      lua.push("len");
      lua.push(set.size());
      lua.setTable(-3);
   }

   @Override
   public <T> void set(Set<?> set, JavaObjectSerializer<T> serializer) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public void map(Map<?, ?> map) {
      map(map, Resp3Type.BULK_STRING); // This is currently only called with an empty map, so we don't care about the type
   }

   @Override
   public void map(Map<?, ?> map, Resp3Type contentType) {
      map(map, contentType, contentType);
   }

   @Override
   public void map(Map<?, ?> map, Resp3Type keyType, Resp3Type valueType) {
      /*
       * A map is represented as a lua table with two elements: 'map', containing a table
       * with the map entries, and `len` containing the size of the set.
       */
      lua.checkStack(3);
      lua.newTable();
      lua.push("map");
      lua.newTable();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
         keyType.serialize(entry.getKey(), this);
         valueType.serialize(entry.getValue(), this);
         lua.setTable(-3);
      }
      lua.setTable(-3);
      lua.push("len");
      lua.push(map.size());
      lua.setTable(-3);
   }

   @Override
   public <T> void map(Map<?, ?> map, JavaObjectSerializer<T> serializer) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public void error(CharSequence value) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public void error(Throwable t) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public <T> void write(T object, JavaObjectSerializer<T> serializer) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public <T> void write(JavaObjectSerializer<T> serializer) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public void serialize(Object object) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }

   @Override
   public void writeNumericPrefix(byte symbol, long number, int additionalWidth) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(0));
   }
}
