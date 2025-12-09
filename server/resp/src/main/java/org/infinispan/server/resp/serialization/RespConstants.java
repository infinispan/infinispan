package org.infinispan.server.resp.serialization;

import java.nio.charset.StandardCharsets;

public interface RespConstants {
   byte NULL = '_';
   byte SIMPLE_STRING = '+';
   byte BULK_STRING = '$';
   byte ARRAY = '*';
   byte NUMERIC = ':';
   byte BOOLEAN = '#';
   byte DOUBLE = ',';
   byte BIG_NUMBER = '(';
   byte MAP = '%';
   byte SET = '~';
   String CRLF_STRING = "\r\n";

   byte[] CRLF = CRLF_STRING.getBytes(StandardCharsets.US_ASCII);
   String OK = "OK";
   String QUEUED_REPLY = "QUEUED";
   String TRUE = "true";
   String FALSE = "false";
}
