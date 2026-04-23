package org.infinispan.cli.commands;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.net.ssl.TrustManager;

import org.aesh.command.Command;
import org.aesh.command.CommandDefinition;
import org.aesh.command.CommandException;
import org.aesh.command.CommandResult;
import org.aesh.command.option.Argument;
import org.infinispan.cli.Context;
import org.infinispan.cli.connection.RegexHostnameVerifier;
import org.infinispan.cli.impl.ContextAwareCommandInvocation;
import org.infinispan.cli.util.ZeroSecurityHostnameVerifier;
import org.infinispan.cli.util.ZeroSecurityTrustManager;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.SslConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.Version;
import org.kohsuke.MetaInfServices;

/**
 * MCP stdio transport bridge. Reads JSON-RPC messages from stdin, forwards them
 * to the Infinispan server's MCP endpoint ({@code /v3/mcp}), and writes responses
 * to stdout. Diagnostic messages go to stderr.
 *
 * <p>Usage:
 * <pre>
 *   cli mcp http://localhost:11222
 *   cli mcp mybookmark
 * </pre>
 *
 * @since 16.2
 */
@MetaInfServices(Command.class)
@CommandDefinition(name = "mcp", description = "Starts an MCP stdio transport bridge to an Infinispan server")
public class Mcp extends CliCommand {

   private static final String MCP_ENDPOINT = "/v3/mcp";
   private static final Map<String, String> JSON_HEADERS = Map.of(
         "Content-Type", "application/json",
         "Accept", "application/json"
   );

   @Argument(description = "Server URL or bookmark name (e.g., http://localhost:11222)")
   String connectionString;

   @Override
   protected CommandResult exec(ContextAwareCommandInvocation invocation) throws CommandException {
      Context context = invocation.getContext();

      if (connectionString == null || connectionString.isEmpty()) {
         connectionString = context.getProperty(Context.Property.AUTOCONNECT_URL);
      }
      if (connectionString == null) {
         throw new CommandException("A server URL or bookmark name must be specified");
      }

      RestClient client = null;
      try {
         // Try to resolve as a bookmark first
         if (!connectionString.contains("://") && !"-".equals(connectionString)) {
            Bookmark.ResolvedBookmark bookmark = Bookmark.resolveNonInteractive(
                  context.configPath(), connectionString);
            if (bookmark != null) {
               client = buildRestClient(bookmark);
            }
         }

         // Not a bookmark (or not found), treat as URL
         if (client == null) {
            client = buildRestClient(context, connectionString);
         }

         runStdioBridge(client, System.in, System.out);
         return CommandResult.SUCCESS;
      } catch (CommandException e) {
         throw e;
      } catch (Exception e) {
         throw new CommandException("MCP bridge error: " + e.getMessage(), e);
      } finally {
         Util.close(client);
      }
   }

   private static RestClient buildRestClient(Bookmark.ResolvedBookmark bookmark) throws CommandException {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      try {
         URL url = new URL(bookmark.url());
         int port = url.getPort();
         builder.addServer().host(url.getHost()).port(port > 0 ? port : url.getDefaultPort());
         boolean https = "https".equals(url.getProtocol());

         if (https || bookmark.truststore() != null || bookmark.trustAll()) {
            SslConfigurationBuilder ssl = builder.security().ssl().enable();
            if (bookmark.trustAll()) {
               ssl.trustManagers(new TrustManager[]{new ZeroSecurityTrustManager()})
                     .hostnameVerifier(new ZeroSecurityHostnameVerifier());
            } else {
               if (bookmark.truststore() != null) {
                  ssl.trustStoreFileName(bookmark.truststore());
                  if (bookmark.truststorePassword() != null) {
                     ssl.trustStorePassword(bookmark.truststorePassword().toCharArray());
                  }
               }
               if (bookmark.hostnameVerifier() != null) {
                  ssl.hostnameVerifier(new RegexHostnameVerifier(bookmark.hostnameVerifier()));
               }
            }
            if (bookmark.keystore() != null) {
               ssl.keyStoreFileName(bookmark.keystore());
               if (bookmark.keystorePassword() != null) {
                  ssl.keyStorePassword(bookmark.keystorePassword().toCharArray());
               }
            }
         }

         if (bookmark.username() != null) {
            builder.security().authentication().enable()
                  .username(bookmark.username())
                  .password(bookmark.password());
         }
      } catch (Exception e) {
         throw new CommandException("Invalid bookmark URL: " + bookmark.url(), e);
      }
      builder.header("User-Agent", Version.getBrandName() + " CLI/MCP " + Version.getBrandVersion());
      return RestClient.forConfiguration(builder.build());
   }

   private static RestClient buildRestClient(Context context, String connectionUrl) throws CommandException {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      try {
         if ("-".equals(connectionUrl)) {
            builder.addServer().host("localhost").port(11222);
         } else {
            URL url = new URL(connectionUrl);
            int port = url.getPort();
            builder.addServer().host(url.getHost()).port(port > 0 ? port : url.getDefaultPort());
            String userInfo = url.getUserInfo();
            if (userInfo != null) {
               String[] split = userInfo.split(":");
               builder.security().authentication().enable()
                     .username(URLDecoder.decode(split[0], StandardCharsets.UTF_8));
               if (split.length == 2) {
                  builder.security().authentication()
                        .password(URLDecoder.decode(split[1], StandardCharsets.UTF_8));
               }
            }
            if ("https".equals(url.getProtocol())) {
               configureSsl(builder, context);
            }
         }
      } catch (Exception e) {
         throw new CommandException("Invalid server URL: " + connectionUrl, e);
      }
      builder.header("User-Agent", Version.getBrandName() + " CLI/MCP " + Version.getBrandVersion());
      return RestClient.forConfiguration(builder.build());
   }

   private static void configureSsl(RestClientConfigurationBuilder builder, Context context) {
      SslConfigurationBuilder ssl = builder.security().ssl().enable();
      if (Boolean.parseBoolean(context.getProperty(Context.Property.TRUSTALL))) {
         ssl.trustManagers(new TrustManager[]{new ZeroSecurityTrustManager()})
               .hostnameVerifier(new ZeroSecurityHostnameVerifier());
      } else {
         String truststore = context.getProperty(Context.Property.TRUSTSTORE);
         if (truststore != null) {
            ssl.trustStoreFileName(truststore);
            String truststorePassword = context.getProperty(Context.Property.TRUSTSTORE_PASSWORD);
            if (truststorePassword != null) {
               ssl.trustStorePassword(truststorePassword.toCharArray());
            }
         }
      }
      String keystore = context.getProperty(Context.Property.KEYSTORE);
      if (keystore != null) {
         ssl.keyStoreFileName(keystore);
         String keystorePassword = context.getProperty(Context.Property.KEYSTORE_PASSWORD);
         if (keystorePassword != null) {
            ssl.keyStorePassword(keystorePassword.toCharArray());
         }
      }
   }

   /**
    * Runs the MCP stdio bridge loop, reading JSON-RPC lines from the given input stream,
    * forwarding them to the server, and writing responses to the given output stream.
    *
    * @param client the REST client connected to the Infinispan server
    * @param in     input stream to read JSON-RPC messages from (one per line)
    * @param out    output stream to write JSON-RPC responses to
    */
   public static void runStdioBridge(RestClient client, InputStream in, PrintStream out) throws IOException, CommandException {
      String endpoint = client.getConfiguration().contextPath() + MCP_ENDPOINT;
      System.err.println("Infinispan MCP bridge started. Waiting for JSON-RPC messages on stdin...");
      BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
      String line;
      while ((line = reader.readLine()) != null) {
         if (line.isBlank()) {
            continue;
         }
         try {
            CompletionStage<RestResponse> responseFuture = client.raw()
                  .post(endpoint, JSON_HEADERS, RestEntity.create(MediaType.APPLICATION_JSON, line));
            RestResponse response = responseFuture.toCompletableFuture().get();
            int status = response.status();
            String body = response.body();
            if (status >= 200 && status < 300 && body != null && !body.isEmpty()) {
               out.println(body);
               out.flush();
            } else if (status >= 400) {
               String msg = body != null && !body.isEmpty() ? body : "HTTP " + status;
               String errorResponse = "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32603,\"message\":\""
                     + escapeJson(msg) + "\"},\"id\":null}";
               out.println(errorResponse);
               out.flush();
            }
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CommandException("Interrupted while waiting for server response", e);
         } catch (Exception e) {
            String errorResponse = "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":-32603,\"message\":\""
                  + escapeJson(e.getMessage()) + "\"},\"id\":null}";
            out.println(errorResponse);
            out.flush();
            System.err.println("Error forwarding request: " + e.getMessage());
         }
      }
      System.err.println("Infinispan MCP bridge stopped (stdin closed).");
   }

   private static String escapeJson(String s) {
      if (s == null) return "Internal error";
      return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
   }
}
