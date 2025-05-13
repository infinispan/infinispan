package org.infinispan.server.resp.scripting;

import static org.infinispan.server.resp.scripting.LuaTaskEngine.fName;
import static party.iroiro.luajava.lua51.Lua51Consts.LUA_GLOBALSINDEX;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.infinispan.commons.CacheListenerException;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.infinispan.remoting.RemoteException;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespVersion;
import org.infinispan.server.resp.logging.Log;
import org.jboss.logging.Logger;

import io.netty.channel.ChannelHandlerContext;
import party.iroiro.luajava.JFunction;
import party.iroiro.luajava.Lua;
import party.iroiro.luajava.lua51.Lua51;
import party.iroiro.luajava.lua51.Lua51Consts;

/**
 * LuaContext manages a {@link Lua} instance.
 */
public class LuaContext implements AutoCloseable {
   public enum Mode {
      USER,
      LOAD
   }

   private static final String REDIS_API_NAME = "redis";

   private static final String[] LIBRARIES_ALLOW_LIST = {"string", "math", "table", "os"}; // bit, cjson, cmsgpack, struct are not available in luajava
   private static final String[] REDIS_API_ALLOW_LIST = {"redis", "__redis__err__handler"};
   private static final String[] LUA_BUILTINS_ALLOW_LIST = {"xpcall", "tostring", "getfenv", "setmetatable", "next", "assert", "tonumber", "rawequal", "collectgarbage", "getmetatable", "rawset", "pcall", "coroutine", "type", "_G", "select", "unpack", "gcinfo", "pairs", "rawget", "loadstring", "ipairs", "_VERSION", "setfenv", "load", "error"};
   private static final String[] LUA_BUILTINS_NOT_DOCUMENTED_ALLOW_LIST = {"newproxy"};
   private static final String[] LUA_BUILTINS_REMOVED_AFTER_INITIALIZATION_ALLOW_LIST = {"debug"};
   private static final Set<String> ALLOW_LISTS;
   private static final Set<String> DENY_LIST = Set.of("dofile", "loadfile", "print");

   public static final int LOG_DEBUG = 0; // TRACE
   public static final int LOG_VERBOSE = 1; // DEBUG
   public static final int LOG_NOTICE = 2; // INFO
   public static final int LOG_WARNING = 3; // WARN
   private static final Logger.Level[] LEVEL_MAP = {Logger.Level.TRACE, Logger.Level.DEBUG, Logger.Level.INFO, Logger.Level.WARN};

   public static final int PROPAGATE_AOF = 1;
   public static final int PROPAGATE_REPL = 2;
   public static final int PROPAGATE_NONE = 0;
   public static final int PROPAGATE_ALL = PROPAGATE_AOF | PROPAGATE_REPL;

   static {
      ALLOW_LISTS = new HashSet<>();
      ALLOW_LISTS.addAll(Arrays.asList(LIBRARIES_ALLOW_LIST));
      ALLOW_LISTS.addAll(Arrays.asList(REDIS_API_ALLOW_LIST));
      ALLOW_LISTS.addAll(Arrays.asList(LUA_BUILTINS_ALLOW_LIST));
      ALLOW_LISTS.addAll(Arrays.asList(LUA_BUILTINS_NOT_DOCUMENTED_ALLOW_LIST));
      ALLOW_LISTS.addAll(Arrays.asList(LUA_BUILTINS_REMOVED_AFTER_INITIALIZATION_ALLOW_LIST));
   }

   final Lua lua;

   // context variables
   long flags;
   Resp3Handler handler;
   ChannelHandlerContext ctx;
   Mode mode = Mode.USER;
   LuaContextPool pool;

   LuaContext() {
      lua = new Lua51();
      for (String lib : LIBRARIES_ALLOW_LIST) {
         lua.openLibrary(lib);
      }
      lua.openLibrary("debug");
      installMathRandom();
      installErrorHandler();
      installRedisAPI();
      luaSetErrorMetatable();
   }

   private void installRedisAPI() {
      lua.newTable();

      // redis.sha1hex(string)
      tableAdd(lua, "sha1hex", l -> {
         int argc = l.getTop();
         if (argc != 1) {
            l.error("wrong number of arguments");
         }
         String hex = sha1hex(l.toString(1));
         l.push(hex);
         return 1;
      });
      // redis.call(string, ...)
      tableAdd(lua, "call", l -> executeRespCommand(l, true));
      // redis.pcall(string, ...)
      tableAdd(lua, "pcall", l -> executeRespCommand(l, false));
      // redis.setresp(int)
      tableAdd(lua, "setresp", l -> {
         int argc = l.getTop();
         if (argc != 1) {
            l.error("redis.setresp() requires one argument.");
         }
         try {
            handler.writer().version(RespVersion.of((int) l.toInteger(-argc)));
         } catch (IllegalArgumentException e) {
            l.error("RESP version must be 2 or 3.");
         }
         return 0;
      });
      // redis.error_reply(string)
      tableAdd(lua, "error_reply", l -> {
         if (l.getTop() != 1 || l.type(-1) != Lua.LuaType.STRING) {
            l.error("wrong number or type of arguments");
            return 1;
         }
         String err = l.toString(-1);
         if (!err.startsWith("-")) {
            err = "-" + err;
         }
         luaPushError(lua, err);
         return 1;
      });
      // redis.status_reply(string)
      tableAdd(lua, "status_reply", l -> {
         if (l.getTop() != 1 || l.type(-1) != Lua.LuaType.STRING) {
            l.error("wrong number or type of arguments");
         }
         l.newTable();
         l.push("ok");
         l.pushValue(-3);
         l.setTable(-3);
         return 1;
      });
      // redis.set_repl(int)
      tableAdd(lua, "set_repl", l -> {
         int argc = l.getTop();
         if (argc != 1) {
            l.error("redis.set_repl() requires one argument.");
         }
         long flags = l.toInteger(-1);
         if ((flags & ~PROPAGATE_ALL) != 0) {
            l.error("Invalid replication flags. Use REPL_AOF, REPL_REPLICA, REPL_ALL or REPL_NONE.");
         }
         // TODO: set repl flags
         return 0;
      });
      tableAdd(lua, "REPL_NONE", PROPAGATE_NONE);
      tableAdd(lua, "REPL_AOF", PROPAGATE_AOF);
      tableAdd(lua, "REPL_SLAVE", PROPAGATE_REPL);
      tableAdd(lua, "REPL_REPLICA", PROPAGATE_REPL);
      tableAdd(lua, "REPL_ALL", PROPAGATE_ALL);
      tableAdd(lua, "log", l -> {
         int j, argc = l.getTop();
         if (argc < 2) {
            luaPushError(lua, "redis.log() requires two arguments or more.");
            return -1;
         } else if (!l.isNumber(-argc)) {
            luaPushError(lua, "First argument must be a number (log level).");
            return -1;
         }
         int level = (int) l.toInteger(-argc);
         if (level < LOG_DEBUG || level > LOG_WARNING) {
            luaPushError(lua, "Invalid log level.");
            return -1;
         }
         StringBuilder sb = new StringBuilder();
         for (j = 1; j < argc; j++) {
            sb.append(l.toString(j - argc));
         }
         Log.SERVER.log(LEVEL_MAP[level], sb);
         return 0;
      });
      tableAdd(lua, "LOG_DEBUG", LOG_DEBUG);
      tableAdd(lua, "LOG_VERBOSE", LOG_VERBOSE);
      tableAdd(lua, "LOG_NOTICE", LOG_NOTICE);
      tableAdd(lua, "LOG_WARNING", LOG_WARNING);
      tableAdd(lua, "REDIS_VERSION_NUM", Version.getVersionShort());
      tableAdd(lua, "REDIS_VERSION", Version.getVersion());
      // Give a name to the table
      lua.setGlobal(REDIS_API_NAME);
   }

   private void installMathRandom() {
      lua.getGlobal("math");
      lua.push("random");
      lua.push(l -> {
         switch (l.getTop()) {
            case 0: {
               lua.push(handler.respServer().random().nextDouble());
               break;
            }
            case 1: {
               long upper = lua.toInteger(1);
               if (upper <= 1) {
                  lua.error("interval is empty");
               }
               lua.push(handler.respServer().random().nextLong(1, upper));
               break;
            }
            case 2: {
               long lower = lua.toInteger(1);
               long upper = lua.toInteger(2);
               lua.push(handler.respServer().random().nextLong(lower, upper));
               break;
            }
            default:
               lua.error("wrong number of arguments");
         }
         return 1;
      });
      lua.setTable(-3);
      lua.push("randomseed");
      lua.push(l -> {
         handler.respServer().random().setSeed(l.toInteger(1));
         return 0;
      });
      lua.setTable(-3);
      lua.setGlobal("math");
   }

   private void installErrorHandler() {
      String err_handler = """
            -- copy the `debug` global to a local, and nil it so it cannot be used by user scripts
            local dbg = debug
            debug = nil
            function __redis__err__handler(err)
              -- get debug information for the previous call (type, source and line)
              local i = dbg.getinfo(2,'nSl')
              -- if it was a native call, get the information for the previous element in the stack
              if i and i.what == 'C' then
                i = dbg.getinfo(3,'nSl')
              end
              if type(err) ~= 'table' then
                err = {err='ERR ' .. tostring(err)}
              end
              if i then
                err['source'] = i.source
                err['line'] = i.currentline
              end
              return err
            end
            """;
      byte[] bytes = err_handler.getBytes(StandardCharsets.US_ASCII);
      ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
      buffer.put(bytes);
      lua.load(buffer, "@err_handler_def");
      lua.pCall(0, 0);
   }

   public int executeRespCommand(Lua l, boolean raiseError) {
      int argc = l.getTop();
      String command = l.toString(-argc);
      RespCommand respCommand = RespCommand.fromString(command);
      if (respCommand == null) {
         l.push("Unknown Redis command called from script");
         return -1;
      }
      long commandMask = respCommand.aclMask();
      if (AclCategory.CONNECTION.matches(commandMask)){
         l.push("This Redis command is not allowed from script");
         return -1;
      }
      if (ScriptFlags.NO_WRITES.isSet(flags) && AclCategory.WRITE.matches(commandMask)) {
         l.push("Write commands are not allowed from read-only scripts.");
         return -1;
      }
      List<byte[]> args = new ArrayList<>(argc - 1);
      for (int i = -argc + 1; i < 0; i++) {
         args.add(l.toString(i).getBytes(StandardCharsets.US_ASCII));
      }
      CompletableFuture<RespRequestHandler> future = handler.handleRequest(ctx, respCommand, args).toCompletableFuture();
      try {
         future.get(); // TODO: handle timeouts ?
      } catch (Throwable t) {
         Throwable cause = filterCause(t);
         Log.SERVER.debugf(cause, "Error while processing command '%s'", respCommand);
         return -1;
      }
      if (lua.type(-1) == Lua.LuaType.TABLE) {
         lua.push("err");
         lua.rawGet(-2);
         if (lua.type(-1) == Lua.LuaType.STRING && raiseError) {
            String error = lua.toString(-1);
            lua.pop(2);
            lua.error(error);
         }
         lua.pop(1);
      }
      return 1;
   }

   public static Throwable filterCause(Throwable re) {
      if (re == null) return null;
      Class<? extends Throwable> tClass = re.getClass();
      Throwable cause = re.getCause();
      if (cause != null && (tClass == ExecutionException.class || tClass == CompletionException.class || tClass == InvocationTargetException.class || tClass == RemoteException.class || tClass == RuntimeException.class || tClass == CacheListenerException.class))
         return filterCause(cause);
      else
         return re;
   }

   /**
    * Returns this instance to the pool it was borrowed from
    */
   @Override
   public void close() {
      if (pool != null) {
         pool.returnToPool(this);
      }
   }

   /**
    * Releases the Lua context. This is only called by {@link LuaContextPool}
    */
   void shutdown() {
      pool = null;
      lua.close();
   }

   // Internal methods

   private static void tableAdd(Lua lua, String name, JFunction function) {
      lua.push(name);
      lua.push(function);
      lua.setTable(-3);
   }

   private static void tableAdd(Lua lua, String name, int i) {
      lua.push(name);
      lua.push(i);
      lua.setTable(-3);
   }

   private static void tableAdd(Lua lua, String name, String value) {
      lua.push(name);
      lua.push(value);
      lua.setTable(-3);
   }

   public static String sha1hex(String s) {
      try {
         MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
         return Util.toHexString(sha1.digest(s.getBytes(StandardCharsets.UTF_8)));
      } catch (NoSuchAlgorithmException e) {
         throw new RuntimeException(e);
      }
   }

   /*
    * There are two possible formats for the received `error` string:
    * 1) "-CODE msg": in this case we remove the leading '-' since we don't store it as part of the lua error format.
    * 2) "msg": in this case we prepend a generic 'ERR' code since all error statuses need some error code.
    */
   public static void luaPushError(Lua lua, String error) {
      String msg;
      // Trim any CR/LF at the end
      int endpos = error.length() - (error.endsWith("\r\n") ? 2 : 0);
      if (error.startsWith("-")) {
         int pos = error.indexOf(' ');
         if (pos < 0) {
            msg = "ERR " + error.substring(1, endpos);
         } else {
            msg = error.substring(1, endpos);
         }
      } else {
         msg = "ERR " + error.substring(0, endpos);
      }
      lua.newTable();
      tableAdd(lua, "err", msg);
   }

   private static int luaProtectedTableError(Lua lua) {
      int argc = lua.getTop();
      if (argc != 2) {
         lua.error("Wrong number of arguments to luaProtectedTableError");
      }
      if (!lua.isString(-1) && !lua.isNumber(-1)) {
         lua.error("Second argument to luaProtectedTableError must be a string or number");
      }
      String variableName = lua.toString(-1);
      lua.error("Script attempted to access nonexistent global variable '" + variableName + "'");
      return 0;
   }

   private void luaSetErrorMetatable() {
      lua.push(LUA_GLOBALSINDEX);
      lua.newTable();
      lua.push(LuaContext::luaProtectedTableError);
      lua.setField(-2, "__index");
      lua.setMetatable(-2);
      lua.pop(1);
   }

   private static int luaNewIndexAllowList(Lua lua) {
      int argc = lua.getTop();
      if (argc != 3) {
         lua.error("Wrong number of arguments to luaNewIndexAllowList"); // Same error Redis reports
      }
      if (!lua.isTable(-3)) {
         lua.error("first argument to luaNewIndexAllowList must be a table");
      }
      if (!lua.isString(-2) && !lua.isNumber(-2)) {
         lua.error("Second argument to luaNewIndexAllowList must be a string or number");
      }
      String variableName = lua.toString(-2);
      if (ALLOW_LISTS.contains(variableName)) {
         lua.rawSet(-3);
      } else if (!DENY_LIST.contains(variableName)) {
         Log.SERVER.warnf("A key '%s' was added to Lua globals which is not on the globals allow list nor listed on the deny list.", variableName);
      }
      return 0;
   }

   private static int luaSetAllowListProtection(Lua lua) {
      lua.newTable();
      lua.push(LuaContext::luaNewIndexAllowList);
      lua.setField(-2, "__newindex");
      lua.setMetatable(-2);
      return 0;
   }

   void registerScript(LuaCode code) {
      String name = fName(code.sha());
      lua.getField(Lua51Consts.LUA_REGISTRYINDEX, name);
      if (lua.get().type() == Lua.LuaType.NIL) {
         byte[] bytes = code.code().getBytes(StandardCharsets.US_ASCII);
         ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
         buffer.put(bytes);
         lua.load(buffer, "@user_script");
         lua.setField(Lua51Consts.LUA_REGISTRYINDEX, name);
      }
   }

   void unregisterScript(LuaCode code) {
      lua.pushNil();
      lua.setField(Lua51Consts.LUA_REGISTRYINDEX, fName(code.sha()));
   }
}
