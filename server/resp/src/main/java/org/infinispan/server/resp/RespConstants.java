package org.infinispan.server.resp;

import java.nio.charset.StandardCharsets;

public interface RespConstants {
   byte SIMPLE_STRING = '+';
   byte BULK_STRING = '$';
   byte ARRAY = '*';
   byte NUMERIC = ':';
   byte HELLO = '@';
   String CRLF = "\r\n";

   byte[] OK = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);
}
