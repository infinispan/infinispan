package org.infinispan.server.resp;

import java.nio.charset.StandardCharsets;

public interface RespConstants {
   byte SIMPLE_STRING = '+';
   byte BULK_STRING = '$';
   byte ARRAY = '*';
   byte NUMERIC = ':';
   byte HELLO = '@';
   String CRLF_STRING = "\r\n";

   byte[] CRLF = CRLF_STRING.getBytes(StandardCharsets.US_ASCII);

   byte[] OK = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);
   byte[] NIL = "$-1\r\n".getBytes(StandardCharsets.US_ASCII);
   byte[] QUEUED_REPLY = "+QUEUED\r\n".getBytes(StandardCharsets.US_ASCII);
}
