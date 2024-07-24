package org.infinispan.server.resp;

import java.nio.charset.StandardCharsets;

public interface RespConstants {
   byte SIMPLE_STRING = '+';
   byte BULK_STRING = '$';
   byte ARRAY = '*';
   byte NUMERIC = ':';
   byte HELLO = '@';
   String CRLF_STRING = "\r\n";
   String NULL_STRING = "_" + CRLF_STRING;

   byte[] CRLF = CRLF_STRING.getBytes(StandardCharsets.US_ASCII);

   byte[] OK = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);
   byte[] NULL = NULL_STRING.getBytes(StandardCharsets.US_ASCII);
   byte[] QUEUED_REPLY = "+QUEUED\r\n".getBytes(StandardCharsets.US_ASCII);
}
