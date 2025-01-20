package org.infinispan.server.resp.serialization.lua;

import static org.infinispan.server.resp.scripting.LuaContext.filterCause;
import static org.infinispan.server.resp.scripting.LuaContext.luaPushError;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.jdkspecific.CallerId;
import org.infinispan.server.resp.RespVersion;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.serialization.SerializationHint;

import party.iroiro.luajava.Lua;

public class LuaResponseWriter implements ResponseWriter {
   private final Lua lua;
   private RespVersion version = RespVersion.RESP3; // TODO: once we support RESP2, this should be lowered
   private int arrayIndex = 0;

   public LuaResponseWriter(Lua lua) {
      this.lua = lua;
   }

   @Override
   public RespVersion version() {
      return version;
   }

   @Override
   public void version(RespVersion version) {
      this.version = version;
   }

   @Override
   public boolean isInternal() {
      return true;
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
      if (value == null) {
         lua.pushNil();
      } else {
         lua.push(value.toString());
      }
   }

   @Override
   public void string(CharSequence value) {
      lua.checkStack(1);
      if (value == null) {
         lua.pushNil();
      } else {
         lua.push(value.toString());
      }
   }

   @Override
   public void string(byte[] value) {
      lua.checkStack(1);
      if (value == null) {
         lua.pushNil();
      } else {
         // We use ISO_8859_1 as that won't perform any conversions when using compact strings (the default)
         lua.push(new String(value, StandardCharsets.ISO_8859_1));
      }
   }

   @Override
   public void integers(Number value) {
      lua.checkStack(1);
      if (value == null) {
         lua.pushNil();
      } else {
         lua.push(value);
      }
   }

   @Override
   public void doubles(Number value) {
      lua.checkStack(1);
      if (value == null) {
         lua.pushNil();
      } else {
      lua.push(value);
      }
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
      if (collection == null) {
         nulls();
      } else {
         lua.checkStack(2);
         lua.newTable();
         int i = 1;
         for (T o : collection) {
            // push the array index as the "key" in the table
            lua.push(i);
            // push the value
            serializer.accept(o, this);
            // push the top two elements on the stack (key at -2 and value at -1) to the table (at -3)
            lua.setTable(-3);
            i++;
         }
      }
   }

   @Override
   public void array(Collection<?> collection, Resp3Type contentType) {
      if (collection == null) {
         nulls();
      } else {
         lua.checkStack(2);
         lua.newTable();
         int i = 1;
         for (Object o : collection) {
            // push the array index as the "key" in the table
            lua.push(i);
            // push the value
            contentType.serialize(o, this);
            // push the top two elements on the stack (key at -2 and value at -1) to the table (at -3)
            lua.setTable(-3);
            i++;
         }
      }
   }

   @Override
   public void emptySet() {
      set(Collections.emptySet(), Resp3Type.BULK_STRING);
   }

   @Override
   public void set(Set<?> set, Resp3Type contentType) {
      if (set == null) {
         nulls();
      } else {
         /*
          * A set is represented as a lua table with two elements: 'set', containing a table
          * with the set entries as keys and a simple true boolean as value, and `len` containing the size of the set.
          */
         lua.checkStack(3);
         // create the "outer" table
         lua.newTable();
         lua.push("set");
         // create the actual "set" table
         lua.newTable();
         for (Object o : set) {
            // push the object as the "key" in the "set" table
            contentType.serialize(o, this);
            lua.checkStack(1);
            // we need a value, just use a boolean
            lua.push(true);
            // push the top two elements on the stack (object at -2 and the boolean at -1) to the table (at -3)
            lua.setTable(-3);
         }
         // push the "set" table to the "outer" table
         lua.setTable(-3);
         lua.push("len");
         lua.push(set.size());
         // push the "len" item to the "outer" table
         lua.setTable(-3);
      }
   }

   @Override
   public <T> void set(Set<?> set, JavaObjectSerializer<T> serializer) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
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
      if (map == null) {
         nulls();
      } else {
         /*
          * A map is represented as a lua table with two elements: 'map', containing a table
          * with the map entries, and `len` containing the size of the set.
          */
         lua.checkStack(3);
         // create the "outer" table
         lua.newTable();
         lua.push("map");
         // create the actual "map" table
         lua.newTable();
         for (Map.Entry<?, ?> entry : map.entrySet()) {
            // push the key
            keyType.serialize(entry.getKey(), this);
            // push the value
            valueType.serialize(entry.getValue(), this);
            // push the top two elements on the stack (key at -2 and value at -1) to the table (at -3)
            lua.setTable(-3);
         }
         // push the "map" table to the "outer" table
         lua.setTable(-3);
         lua.push("len");
         lua.push(map.size());
         // push the "len" item to the "outer" table
         lua.setTable(-3);
      }
   }

   @Override
   public void map(Map<?, ?> map, SerializationHint.KeyValueHint keyValueHint) {
      if (map == null) {
         nulls();
      } else {
         /*
          * A map is represented as a lua table with two elements: 'map', containing a table
          * with the map entries, and `len` containing the size of the set.
          */
         lua.checkStack(3);
         // create the "outer" table
         lua.newTable();
         lua.push("map");
         // create the actual "map" table
         lua.newTable();
         for (Map.Entry<?, ?> entry : map.entrySet()) {
            // push the key
            keyValueHint.key().serialize(entry.getKey(), this);
            // push the value
            keyValueHint.value().serialize(entry.getValue(), this);
            // push the top two elements on the stack (key at -2 and value at -1) to the table (at -3)
            lua.setTable(-3);
         }
         // push the "map" table to the "outer" table
         lua.setTable(-3);
         lua.push("len");
         lua.push(map.size());
         // push the "len" item to the "outer" table
         lua.setTable(-3);
      }
   }

   @Override
   public void error(CharSequence value) {
      lua.checkStack(3);
      // create a table with the "err" entry
      luaPushError(lua, value.toString());
      // add `ignore_error_stats_update = true` to the table
      lua.push("ignore_error_stats_update");
      lua.push(true);
      lua.setTable(-3);
   }

   @Override
   public void error(Throwable t) {
      error(filterCause(t).getMessage());
   }

   @Override
   public <T> void write(T object, JavaObjectSerializer<T> serializer) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public <T> void write(JavaObjectSerializer<T> serializer) {
      serializer.accept(null, this);
   }

   @Override
   public void serialize(Object object) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public void writeNumericPrefix(byte symbol, long number) {
      throw new UnsupportedOperationException(CallerId.getCallerMethodName(1));
   }

   @Override
   public void arrayStart(int size) {
      assert arrayIndex == 0;
      lua.checkStack(2);
      lua.newTable();
      arrayIndex = 1;
   }

   @Override
   public void arrayEnd() {
      if (arrayIndex > 1) {
         // expects the stack to contain an array index (at -2) and a value (at -1) and adds them to the array table
         lua.setTable(-3);
      }
      arrayIndex = 0;
   }

   @Override
   public void arrayNext() {
      if (arrayIndex > 1) {
         // expects the stack to contain an array index (at -2) and a value (at -1) and adds them to the array table
         lua.setTable(-3);
      }
      // push the next array index on the stack
      lua.push(arrayIndex++);
   }
}
