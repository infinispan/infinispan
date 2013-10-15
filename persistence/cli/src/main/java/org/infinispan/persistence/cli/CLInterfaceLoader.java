package org.infinispan.persistence.cli;

import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.cli.Context;
import org.infinispan.cli.commands.Command;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.ConnectionFactory;
import org.infinispan.cli.impl.CommandBufferImpl;
import org.infinispan.cli.impl.ContextImpl;
import org.infinispan.cli.io.IOAdapter;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.cli.configuration.CLInterfaceLoaderConfiguration;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * A read-only cache loader retrieving data from another cache(s) using the
 * Command Line Interface.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class CLInterfaceLoader<K, V> implements CacheLoader<K, V> {

   private InitializationContext ctx;
   private Connection connection;
   private CLInterfaceLoaderConfiguration cfg;

   private ObjectMapper jsonMapper = new ObjectMapper().enableDefaultTyping(
         ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.WRAPPER_OBJECT);

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      this.cfg = ctx.getConfiguration();
   }

   @Override
   public MarshalledEntry<K, V> load(K key) {
      // TODO: a CLI command to retrieve value + metadata is needed
      ProcessedCommand parsed = new ProcessedCommand("get " + key.toString() + ";");

      // Having CLI context as field of this cache loader produces all sorts of
      // issues when paralell requests are received, since the commands get
      // bunched up. This is valid in shell mode, but a bit problematic for
      // the cache loader. So, instead create a context instance for each
      // operation. This is obviously not very efficient, but this cache loader
      // should only be used during migration, so any inefficiencies should
      // only be temporary.
      Context cliCtx = createContext();
      Command command = cliCtx.getCommandRegistry().getCommand(parsed.getCommand());
      command.execute(cliCtx, parsed);
      ResponseMatcher.Result result = ((ResponseMatcher) cliCtx.getOutputAdapter())
            .getResult(Collections.singletonList(parsed));
      if (result.isError)
         throw new CacheException("Unable to load entry: " + result.result);

      if (result.result.equals("null"))
         return null;

      // The value returned could be JSON when custom classes are used,
      // so try reading it. If the read fails, just use the given String.
      Object value = result.result;
      try {
         value = jsonMapper.readValue(result.result, Object.class);
      } catch (IOException e) {
         // Ignore if it fails
      }

      return ctx.getMarshalledEntryFactory().newMarshalledEntry(key, value, null);
   }

   private Context createContext() {
      ResponseMatcher responseMatcher = new ResponseMatcher();
      Context cliCtx = new ContextImpl(responseMatcher, new CommandBufferImpl());
      cliCtx.setConnection(connection);
      return cliCtx;
   }

   @Override
   public boolean contains(K key) {
      return load(key) != null;
   }

   @Override
   public void start() {
      // i.e. "jmx://localhost:2626/Source-CacheManager-1/___defaultcache"
      String serviceUrl = cfg.connectionString();
      connection = ConnectionFactory.getConnection(serviceUrl);
      try {
         connection.connect(null);
      } catch (Exception e) {
         throw new CacheException("Unable to connect to URL: " + serviceUrl, e);
      }
   }


   @Override
   public void stop() {
      if (connection != null && connection.isConnected()) {
         try {
            connection.close();
         } catch (IOException e) {
         }
         connection = null;
      }
   }

   private static class ResponseMatcher implements IOAdapter {

      private static final Log log = LogFactory.getLog(ResponseMatcher.class);

      ConcurrentMap<List<ProcessedCommand>, Result> results = CollectionFactory.makeConcurrentMap();

      @Override
      public boolean isInteractive() {
         return false;
      }

      @Override
      public void error(String error) {
         log.error(error);
      }

      @Override
      public void println(String s) {
         log.debug(s);
      }

      @Override
      public void result(List<ProcessedCommand> commands, String result, boolean isError) {
         results.put(commands, new Result().isError(isError).result(result));
      }

      @Override
      public String readln(String s) throws IOException {
         return null;  // TODO: Customise this generated block
      }

      @Override
      public String secureReadln(String s) throws IOException {
         return null;  // TODO: Customise this generated block
      }

      @Override
      public int getWidth() {
         return 0;  // TODO: Customise this generated block
      }

      @Override
      public void close() throws IOException {
         // TODO: Customise this generated block
      }

      Result getResult(List<ProcessedCommand> commands) {
         return results.remove(commands);
      }

      static class Result {
         boolean isError;
         String result;

         Result isError(boolean isError) {
            this.isError = isError;
            return this;
         }

         Result result(String result) {
            this.result = result;
            return this;
         }
      }
   }

}
