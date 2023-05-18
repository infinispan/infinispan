package org.infinispan.server.resp;

public interface RespConstants {
   byte SIMPLE_STRING = '+';
   byte BULK_STRING = '$';
   byte ARRAY = '*';
   byte NUMERIC = ':';
   byte HELLO = '@';
   String CRLF = "\r\n";
}
