package org.infinispan.server.resp.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.PASSWORD;
import static org.infinispan.server.resp.test.RespTestingUtil.USERNAME;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * A plain socket connection, used to assert on the exact RESP3 bytes the server sends.
 * <p>
 * Clients such as Lettuce can hide differences in the reply type, for example by unwrapping a one-element array into
 * a single value. Use this connection when a test needs to check the reply type itself.
 */
public final class RawRespConnection implements AutoCloseable {
   private final Socket socket;
   private final OutputStream out;
   private final InputStream in;

   private RawRespConnection(String host, int port) throws IOException {
      socket = new Socket(host, port);
      socket.setSoTimeout(10_000);
      out = socket.getOutputStream();
      in = socket.getInputStream();
   }

   /**
    * Opens a connection and switches it to RESP3 with {@code HELLO 3}, authenticating if requested. The {@code HELLO}
    * reply is consumed, so the next frame read belongs to the next command sent.
    */
   public static RawRespConnection connect(String host, int port, boolean authenticate) throws IOException {
      RawRespConnection raw = new RawRespConnection(host, port);
      try {
         raw.send(authenticate ? "HELLO 3 AUTH " + USERNAME + " " + PASSWORD : "HELLO 3");
         // The HELLO reply varies, so skip it by reading up to the reply of a PING.
         raw.send("PING");
         raw.readUntil("+PONG\r\n");
         return raw;
      } catch (IOException | RuntimeException e) {
         raw.close();
         throw e;
      }
   }

   /**
    * Sends a command encoded as a RESP array of bulk strings. The arguments are separated by spaces.
    */
   public void send(String command) throws IOException {
      String[] args = command.split(" ");
      StringBuilder sb = new StringBuilder().append('*').append(args.length).append("\r\n");
      for (String arg : args) {
         sb.append('$').append(arg.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(arg).append("\r\n");
      }
      out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
      out.flush();
   }

   /**
    * Reads exactly as many bytes as {@code expected} contains and asserts they are equal.
    */
   public void assertFrame(String expected) throws IOException {
      byte[] read = in.readNBytes(expected.getBytes(StandardCharsets.UTF_8).length);
      assertThat(new String(read, StandardCharsets.UTF_8)).isEqualTo(expected);
   }

   /**
    * Reads until {@code marker} has been received and returns everything read, including the marker.
    */
   public String readUntil(String marker) throws IOException {
      StringBuilder sb = new StringBuilder();
      while (sb.indexOf(marker) < 0) {
         int b = in.read();
         if (b < 0) throw new EOFException("Connection closed before receiving " + marker);
         sb.append((char) b);
      }
      return sb.toString();
   }

   @Override
   public void close() throws IOException {
      socket.close();
   }
}
