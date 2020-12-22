package org.infinispan.rest;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpContent;

public class ServerSentEvent implements HttpContent {
   private static final byte[] EVENT = "event: ".getBytes();
   private static final byte[] NL = "\n".getBytes();
   private static final byte[] DATA = "data: ".getBytes();

   private final String event;
   private final String data;
   private DecoderResult decoderResult;

   public ServerSentEvent(String event, String data) {
      this.event = event;
      this.data = data;
   }

   @Override
   public String toString() {
      return "ServerSentEvent{" +
            ", event='" + event + '\'' +
            ", data='" + data + '\'' +
            '}';
   }

   @Override
   public HttpContent copy() {
      return this;
   }

   @Override
   public HttpContent duplicate() {
      return this;
   }

   @Override
   public HttpContent retainedDuplicate() {
      return this;
   }

   @Override
   public HttpContent replace(ByteBuf content) {
      return this;
   }

   @Override
   public HttpContent retain() {
      return this;
   }

   @Override
   public HttpContent retain(int increment) {
      return this;
   }

   @Override
   public HttpContent touch() {
      return this;
   }

   @Override
   public HttpContent touch(Object hint) {
      return this;
   }

   @Override
   public ByteBuf content() {
      ByteBuf b = Unpooled.buffer();
      if (event != null) {
         b.writeBytes(EVENT);
         b.writeBytes(event.getBytes(StandardCharsets.UTF_8));
         b.writeBytes(NL);
      }
      for (String line : data.split("\n")) {
         b.writeBytes(DATA);
         b.writeBytes(line.getBytes(StandardCharsets.UTF_8));
         b.writeBytes(NL);
      }
      b.writeBytes(NL);
      return b;
   }

   @Override
   public DecoderResult getDecoderResult() {
      return decoderResult();
   }

   @Override
   public DecoderResult decoderResult() {
      return decoderResult;
   }

   @Override
   public void setDecoderResult(DecoderResult result) {
      this.decoderResult = result;
   }

   @Override
   public int refCnt() {
      return 1;
   }

   @Override
   public boolean release() {
      return false;
   }

   @Override
   public boolean release(int i) {
      return false;
   }
}
