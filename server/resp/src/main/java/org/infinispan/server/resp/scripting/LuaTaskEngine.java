package org.infinispan.server.resp.scripting;

import static org.infinispan.server.resp.scripting.LuaContext.sha1hex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.scripting.impl.ScriptMetadata;
import org.infinispan.scripting.impl.ScriptWithMetadata;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.serialization.SerializationHint;
import org.infinispan.server.resp.serialization.lua.LuaResponseWriter;
import org.infinispan.server.resp.tx.TransactionContext;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.manager.spi.TaskEngine;
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
      this.pool = new LuaContextPool(LuaContext::new, 2, 4);
   }

   public void shutdown() {
      pool.shutdown();
   }

   public CompletionStage<Void> eval(Resp3Handler handler, ChannelHandlerContext ctx, String code, String[] keys, String[] args, long flags) {
      return handler.getBlockingManager().supplyBlocking(() -> {
         LuaContext luaCtx = pool.borrow();
         try {
            LuaCode script = scriptLoad(code, false);
            luaCtx.registerScript(script);
            runScript(luaCtx, handler, ctx, script, keys, args, flags);
            luaCtx.unregisterScript(script);
            return luaCtx;
         } catch (Throwable t) {
            // a throwable here means it was not handled properly by the script execution logic. We discard the lua
            // context since it may be in an unrecoverable state
            luaCtx.shutdown();
            handler.writer().error(t);
            return null;
         }
      }, "eval").thenApplyAsync(luaCtx -> {
         if (luaCtx != null) {
            // Process the lua object on the stack and send it to the actual writer
            luaToResp(luaCtx.lua, handler);
            // Pop the error handler
            luaCtx.lua.pop(1);
            // Return the lua context to the pool
            pool.returnToPool(luaCtx);
         }
         return null;
      }, ctx.channel().eventLoop());
   }

   public CompletionStage<Void> evalSha(Resp3Handler handler, ChannelHandlerContext ctx, String sha, String[] keys, String[] args, long flags) {
      return handler.getBlockingManager().supplyBlocking(() -> {
         LuaContext luaCtx = pool.borrow();
         ScriptWithMetadata script;
         try {
            script = scriptingManager.getScriptWithMetadata(scriptName(sha.toUpperCase()));
         } catch (CacheException e) {
            pool.returnToPool(luaCtx);
            throw new RuntimeException("NOSCRIPT No matching script. Please use EVAL.");
         }
         try {
            LuaCode code = LuaCode.fromScript(script);
            luaCtx.registerScript(code);
            runScript(luaCtx, handler, ctx, code, keys, args, flags);
            return luaCtx;
         } catch (Throwable t) {
            // a throwable here means it was not handled properly by the script execution logic. We discard the lua
            // context since it may be in an unrecoverable state
            luaCtx.shutdown();
            handler.writer().error(t);
            return null;
         }
      }, "evalsha").thenApplyAsync(luaCtx -> {
         if (luaCtx != null) {
            // Process the lua object on the stack and send it to the actual writer
            luaToResp(luaCtx.lua, handler);
            // Pop the error handler
            luaCtx.lua.pop(1);
            pool.returnToPool(luaCtx);
         }
         return null;
      }, ctx.channel().eventLoop());
   }

   private void runScript(LuaContext luaCtx, Resp3Handler handler, ChannelHandlerContext ctx, LuaCode script, String[] keys, String[] args, long flags) {
      luaCtx.handler = handler;
      luaCtx.ctx = ctx;
      luaCtx.flags = flags;
      luaCtx.flags |= script.flags();
      ResponseWriter oldWriter = handler.writer(new LuaResponseWriter(luaCtx.lua));
      try {
         TransactionContext.startTransactionContext(ctx);
         runScript(luaCtx.lua, script, keys, args);
      } catch (Throwable t) {
         throw new RuntimeException(t);
      } finally {
         TransactionContext.endTransactionContext(ctx);
         handler.writer(oldWriter);
      }
   }

   /**
    * Convert the response found on the Lua stack to a RESP response
    */
   private void luaToResp(Lua lua, Resp3Handler handler) {
      try {
         lua.checkStack(4);
      } catch (RuntimeException e) {
         handler.writer().customError("reached lua stack limit");
         lua.pop(1);
         return;
      }
      switch (lua.type(-1)) {
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
            // Is it an error ?
            lua.push("err");
            lua.rawGet(-2);
            if (lua.type(-1) == Lua.LuaType.STRING) {
               lua.pop(1); // pop the error message
               ErrorInfo errorInfo = extractErrorInformation(lua);
               handler.writer().error("-" + errorInfo.message);
               lua.pop(1); // pop the result table
               return;
            }
            lua.pop(1);

            // Is it a simple status ?
            lua.push("ok");
            lua.rawGet(-2);
            if (lua.type(-1) == Lua.LuaType.STRING) {
               String ok = lua.toString(-1).replaceAll("[\\r\\n]", " ");
               handler.writer().string(ok);
               lua.pop(2);
               return;
            }
            lua.pop(1);

            // Is it a double ?
            lua.push("double");
            lua.rawGet(-2);
            if (lua.type(-1) == Lua.LuaType.NUMBER) {
               handler.writer().doubles(lua.toNumber(-1));
               lua.pop(2);
               return;
            }
            lua.pop(1);

            // Is it a map ?
            lua.push("map");
            lua.rawGet(-2);
            if (lua.type(-1) == Lua.LuaType.TABLE) {
               lua.push("len");
               lua.rawGet(-3);
               int size = (int) lua.toInteger(-1);
               lua.pop(1);
               LuaMap<Object, Object> map = new LuaMap<>(lua, -2, size);
               handler.writer().map(map, new SerializationHint.KeyValueHint(
                     (object, writer) -> {
                        lua.pushValue(-2); // duplicate key for iteration
                        luaToResp(lua, handler); // key
                     },
                     (object, writer) -> {
                        luaToResp(lua, handler); // value
                     }
               ));
               lua.pop(2);
               return;
            }
            lua.pop(1);

            // Is it a set ?
            lua.push("set");
            lua.rawGet(-2);
            if (lua.type(-1) == Lua.LuaType.TABLE) {
               lua.push("len");
               lua.rawGet(-3);
               int size = (int) lua.toInteger(-1);
               lua.pop(1);
               LuaSet<Object> set = new LuaSet<>(lua, -2, size);
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
            lua.pop(1);

            // It's an array !
            LuaArray<Object> array = new LuaArray<>(lua, -1);
            handler.writer().array(array, (o, writer) -> luaToResp(lua, handler));
            break;
         default:
            handler.writer().nulls();
      }
      lua.pop(1);
   }

   record ErrorInfo(String message, String source, String line, boolean ignoreStatsUpdate) {
   }

   private String stringField(Lua lua, String name) {
      lua.getField(-1, name);
      try {
         return lua.isString(-1) ? lua.toString(-1) : null;
      } finally {
         lua.pop(1);
      }
   }

   private boolean booleanField(Lua lua, String name) {
      lua.getField(-1, name);
      try {
         return lua.isBoolean(-1) && lua.toBoolean(-1);
      } finally {
         lua.pop(1);
      }
   }

   private ErrorInfo extractErrorInformation(Lua lua) {
      if (lua.isString(-1)) {
         return new ErrorInfo("ERR " + lua.toString(-1), null, null, false);
      } else {
         return new ErrorInfo(
               stringField(lua, "err"),
               stringField(lua, "source"),
               stringField(lua, "line"),
               booleanField(lua, "ignore_error_stats_update")
         );
      }
   }

   static String fName(String sha) {
      return "f_" + sha;
   }

   private void runScript(Lua lua, LuaCode script, String[] keys, String[] args) {
      // Populate the ARGV table and set it as a global
      lua.newTable();
      for (int i = 0; i < args.length; i++) {
         lua.push(args[i]);
         lua.rawSetI(-2, i + 1);
      }
      lua.setGlobal("ARGV");
      // Populate the KEYS table and set it as a global
      lua.newTable();
      for (int i = 0; i < keys.length; i++) {
         lua.push(keys[i]);
         lua.rawSetI(-2, i + 1);
      }
      lua.setGlobal("KEYS");
      lua.getGlobal("__redis__err__handler");
      lua.getField(Lua51Consts.LUA_REGISTRYINDEX, fName(script.sha()));
      // Invoke the function using the supplied error handler which will need to be popped from the stack after
      // the return value has been processed
      lua.getLuaNatives().lua_pcall(lua.getPointer(), 0, 1, -2);
   }

   public LuaCode scriptLoad(String script, boolean persistent) {
      Map<String, String> properties = parseShebang(script, false);
      String sha = sha1hex(script);
      properties.put("sha", sha);
      String name = scriptName(sha);
      ScriptMetadata.Builder builder = new ScriptMetadata.Builder()
            .name(name)
            .extension("lua")
            .language("lua51")
            .properties(properties);
      ScriptMetadata metadata = builder.build();
      if (persistent) {
         scriptingManager.addScript(name, script, metadata);
      }
      return LuaCode.fromScript(script, metadata);
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
      Set<String> names = scriptingManager.getScriptNames();
      for (String name : names) {
         if (name.startsWith("resp_script_")) {
            scriptingManager.removeScript(name);
         }
      }
   }

   private static Map<String, String> parseShebang(String script, boolean required) {
      Map<String, String> properties = new HashMap<>();
      long flags = 0;
      if (script.startsWith("#!")) {
         int end = script.indexOf('\n');
         if (end < 0) {
            throw new IllegalArgumentException("Invalid script shebang");
         }
         String[] parts = script.substring(2, end).split(" ");
         String engine = parts[0];
         if (engine.isBlank()) {
            throw new IllegalArgumentException("Invalid library metadata");
         }
         properties.put("engine", engine.toUpperCase());
         for (int i = 1; i < parts.length; i++) {
            if (parts[i].startsWith("flags=")) {
               String[] fNames = parts[i].substring(6).split(",");
               for (String fName : fNames) {
                  flags |= ScriptFlags.valueOf(fName).value();
               }
            } else if (parts[i].startsWith("name=")) {
               // Process the name
               properties.put("name", parts[i].substring(5));
            } else {
               throw new IllegalArgumentException("Unknown lua shebang option: " + parts[i]);
            }
         }
         if (!properties.containsKey("name") && required) {
            throw new IllegalArgumentException("Library name was not given");
         }
      } else {
         if (required) {
            throw new IllegalArgumentException("Missing library metadata");
         } else {
            flags = ScriptFlags.EVAL_COMPAT_MODE.value();
         }
      }
      properties.put("flags", Long.toString(flags));
      return properties;
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
