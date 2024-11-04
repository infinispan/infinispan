package org.infinispan.server.resp.scripting;

import static org.infinispan.server.resp.scripting.LuaContext.sha1hex;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.scripting.impl.ScriptMetadata;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.serialization.lua.LuaResponseWriter;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.spi.TaskEngine;
import org.infinispan.util.concurrent.BlockingManager;

import io.netty.channel.ChannelHandlerContext;
import party.iroiro.luajava.Lua;
import party.iroiro.luajava.lua51.Lua51Consts;

/**
 * An Infinispan TaskEngine built specifically for executing lua scripts in the context of the resp connector.
 * It is therefore not a generic task engine or a scripting engine that can be used from Hot Rod or REST.
 */
public class LuaTaskEngine implements TaskEngine {

   private final LuaContextPool pool;
   private final ScriptingManager scriptingManager;

   public LuaTaskEngine(ScriptingManager scriptingManager) {
      this.scriptingManager = scriptingManager;
      this.pool = new LuaContextPool(LuaContext::new, 2, 4, 120);
   }

   public void shutdown() {
      pool.shutdown();
   }

   public void eval(Resp3Handler handler, ChannelHandlerContext ctx, String code, String[] keys, String[] args, long flags) {
      LuaScript script = scriptLoad(code, false);
      LuaContext luaCtx = pool.borrow();
      try {
         registerScript(luaCtx, script);
         runScript(luaCtx, handler, ctx, script, keys, args, flags);
      } finally {
         unregisterScript(luaCtx, script);
         pool.returnToPool(luaCtx);
      }
   }

   public void evalSha(Resp3Handler handler, ChannelHandlerContext ctx, String sha, String[] keys, String[] args, long flags) {
      CacheEntry<String, String> entry = scriptingManager.getScriptWithMetadata(scriptName(sha));
      LuaScript script = new LuaScript(entry.getValue(), (ScriptMetadata) entry.getMetadata());
      LuaContext luaCtx = pool.borrow();
      try {
         registerScript(luaCtx, script);
         runScript(luaCtx, handler, ctx, script, keys, args, flags);
      } finally {
         pool.returnToPool(luaCtx);
      }
   }

   private void runScript(LuaContext luaCtx, Resp3Handler handler, ChannelHandlerContext ctx, LuaScript script, String[] keys, String[] args, long flags) {
      luaCtx.handler = handler;
      luaCtx.ctx = ctx;
      luaCtx.flags = flags;
      luaCtx.flags |= script.flags();
      ResponseWriter oldWriter = handler.writer();
      handler.writer(new LuaResponseWriter(luaCtx.lua));
      try {
         runScript(luaCtx.lua, script, keys, args);
      } finally {
         handler.writer(oldWriter);
      }
      // Process the lua object on the stack and send it to the actual writer
      luaToResp(luaCtx.lua, handler);
   }

   /**
    * Convert the response found on the Lua stack to a RESP response
    */
   private void luaToResp(Lua lua, Resp3Handler handler) {
      Lua.LuaType t = lua.type(-1);
      try {
         lua.checkStack(4);
      } catch (RuntimeException e) {
         handler.writer().customError("reached lua stack limit");
         lua.pop(1);
         return;
      }
      switch (t) {
         case STRING:
            handler.writer().string(lua.toString(-1));
            break;
         case BOOLEAN:
            handler.writer().booleans(lua.toBoolean(-1));
            break;
         case NUMBER:
            handler.writer().integers((long) lua.toNumber(-1));
            break;
         case TABLE:
            /* We need to check if it is an array, an error, or a status reply.
             * Error are returned as a single element table with 'err' field.
             * Status replies are returned as single element table with 'ok'
             * field. */

            /* Handle error reply. */
            /* we took care of the stack size on function start */
            lua.push("err");
            lua.rawGet(-2);

            t = lua.type(-1);
            if (t == Lua.LuaType.STRING) {
               lua.pop(1); /* pop the error message, we will use luaExtractErrorInformation to get error information */
               /*errorInfo err_info = {0};
               luaExtractErrorInformation(lua, & err_info);
               addReplyErrorFormatEx(c,
                     err_info.ignore_err_stats_update ? ERR_REPLY_FLAG_NO_STATS_UPDATE : 0,
                     "-%s",
                     err_info.msg);
               luaErrorInformationDiscard( & err_info);*/
               lua.pop(1); /* pop the result table */
               return;
            }
            lua.pop(1); /* Discard field name pushed before. */

            /* Handle status reply. */
            lua.push("ok");
            lua.rawGet(-2);
            t = lua.type(-1);
            if (t == Lua.LuaType.STRING) {
               String ok = lua.toString(-1).replaceAll("[\\r\\n]", " ");
               handler.writer().string(ok);
               lua.pop(2);
               return;
            }
            lua.pop(1); /* Discard field name pushed before. */

            /* Handle double reply. */
            lua.push("double");
            lua.rawGet(-2);
            t = lua.type(-1);
            if (t == Lua.LuaType.NUMBER) {
               handler.writer().doubles(lua.toNumber(-1));
               lua.pop(2);
               return;
            }
            lua.pop(1); /* Discard field name pushed before. */

            /* Handle map reply. */
            lua.push("map");
            lua.rawGet(-2);
            t = lua.type(-1);
            if (t == Lua.LuaType.TABLE) {
               lua.push("len");
               lua.rawGet(-3);
               int size = (int) lua.toInteger(-1);
               LuaMap<Object, Object> map = new LuaMap<>(lua, -3, size);
               handler.writer().map(map, (o, writer) -> {
                  // Stack: table, key, value
                  lua.pushValue(-2); // duplicate key for iteration
                  luaToResp(lua, handler); // key
                  luaToResp(lua, handler); // value
                  // Stack now: table, key
               });
               lua.pop(2);
               return;
            }
            lua.pop(1); /* Discard field name pushed before. */

            /* Handle set reply. */
            lua.push("set");
            lua.rawGet(-2);
            t = lua.type(-1);
            if (t == Lua.LuaType.TABLE) {
               lua.push("len");
               lua.rawGet(-3);
               int size = (int) lua.toInteger(-1);
               LuaSet<Object> set = new LuaSet<>(lua, -3, size);
               handler.writer().set(set, (o, writer) -> {
                  // Stack: table, key (object), value (boolean)
                  lua.pop(1); // Discard the value
                  lua.pushValue(-1); // duplicate the key, for iteration
                  luaToResp(lua, handler); // write the object to the handler
                  // Stack: table, key
               });
               lua.pop(2);
               return;
            }
            lua.pop(1); /* Discard field name pushed before. */

            /* Handle the array reply. */
            LuaCollection<Object> array = new LuaCollection<>(lua, 1);
            handler.writer().array(array, (o, writer) -> luaToResp(lua, handler));
            break;
         default:
            handler.writer().nulls();
      }
      lua.pop(1);
   }

   private static String fName(String sha) {
      return "f_" + sha;
   }

   private void registerScript(LuaContext luaCtx, LuaScript script) {
      Lua lua = luaCtx.lua;
      String name = fName(script.sha());
      lua.getField(Lua51Consts.LUA_REGISTRYINDEX, name);
      if (lua.get().type() == Lua.LuaType.NIL) {
         byte[] bytes = script.code().getBytes(StandardCharsets.US_ASCII);
         ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
         buffer.put(bytes);
         lua.load(buffer, "@user_script");
         lua.setField(Lua51Consts.LUA_REGISTRYINDEX, name);
      }
   }

   private void unregisterScript(LuaContext luaCtx, LuaScript script) {
      luaCtx.lua.pushNil();
      luaCtx.lua.setField(Lua51Consts.LUA_REGISTRYINDEX, fName(script.sha()));
   }

   private void runScript(Lua lua, LuaScript script, String[] keys, String[] args) {
      lua.newTable();
      for (int i = 0; i < args.length; i++) {
         lua.push(args[i]);
         lua.rawSetI(-2, i + 1);
      }
      lua.setGlobal("ARGV");
      lua.newTable();
      for (int i = 0; i < keys.length; i++) {
         lua.push(keys[i]);
         lua.rawSetI(-2, i + 1);
      }
      lua.setGlobal("KEYS");
      lua.getField(Lua51Consts.LUA_REGISTRYINDEX, fName(script.sha()));
      lua.pCall(0, 1);
   }

   public LuaScript scriptLoad(String script, boolean persistent) {
      String sha = sha1hex(script);
      String name = scriptName(sha);
      long flags = parseShebang(script);
      ScriptMetadata.Builder builder = new ScriptMetadata.Builder()
            .name(name)
            .extension("lua")
            .language("lua51")
            .properties(Map.of("sha", sha, "flags", Long.toString(flags)));
      ScriptMetadata metadata = builder.build();
      if (persistent) {
         scriptingManager.addScript(name, script, metadata);
      }
      return new LuaScript(script, metadata);
   }

   private static String scriptName(String sha) {
      return "resp_script_" + sha + ".lua";
   }

   public List<Integer> scriptExists(List<String> shas) {
      List<Integer> exists = new ArrayList<>(shas.size());
      Set<String> names = scriptingManager.getScriptNames();
      for (String sha : shas) {
         exists.add(names.contains(scriptName(sha)) ? 1 : 0);
      }
      return exists;
   }

   public void scriptFlush() {
      //TODO scripts.clear();
   }

   public static long parseShebang(String script) {
      if (script.startsWith("#!")) {
         int end = script.indexOf('\n');
         if (end < 0) {
            throw new IllegalArgumentException("Invalid script shebang");
         }
         String[] parts = script.substring(0, end).split(" ");
         if (!"#!lua".equals(parts[0])) {
            throw new IllegalArgumentException("Unexpected engine in script shebang: " + parts[0]);
         }
         long flags = 0;
         for (int i = 1; i < parts.length; i++) {
            if (parts[i].startsWith("flags=")) {
               String[] fNames = parts[i].substring(6).split(",");
               for (String fName : fNames) {
                  flags |= ScriptFlags.valueOf(fName).value();
               }
            } else if (parts[i].startsWith("name=")) {
               // Process the name
            } else {
               throw new IllegalArgumentException("Unknown lua shebang option: " + parts[i]);
            }
         }
         return flags;
      } else {
         return ScriptFlags.EVAL_COMPAT_MODE.value();
      }
   }

   // TaskEngine methods

   @Override
   public String getName() {
      return "resp-lua-engine";
   }

   @Override
   public List<Task> getTasks() {
      return List.of();
   }

   @Override
   public <T> CompletionStage<T> runTask(String taskName, TaskContext context, BlockingManager blockingManager) {
      return CompletableFutures.completedNull();
   }

   @Override
   public boolean handles(String taskName) {
      return false;
   }
}
