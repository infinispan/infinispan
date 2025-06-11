package org.infinispan.rest.resources.mcp;

public class McpConstants {
   public static final String MCP_SERVER_FEATURE = "mcp";
   // Error codes
   public static final int PARSE_ERROR = -32700;
   public static final int INVALID_REQUEST = -32600;
   public static final int METHOD_NOT_FOUND = -32601;
   public static final int INVALID_PARAMS = -32602;
   public static final int INTERNAL_ERROR = -32603;


   public static final String MCP_SESSION_ID = "Mcp-Session-Id";
   public static final String MCP_VERSION = "2025-03-26";

   // Message constants
   public static final String DESCRIPTION = "description";
   public static final String NAME = "name";
   public static final String TYPE = "type";
   public static final String INPUT_SCHEMA = "inputSchema";
   public static final String RESULT = "result";
   public static final String JSONRPC = "jsonrpc";
   public static final String PARAMS = "params";
}
