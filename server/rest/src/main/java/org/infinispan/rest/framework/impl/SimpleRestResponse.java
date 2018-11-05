package org.infinispan.rest.framework.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.CacheControl;
import org.infinispan.rest.framework.RestResponse;

/**
 * @since 10.0
 */
public class SimpleRestResponse implements RestResponse {

   private final Builder builder;

   private SimpleRestResponse(Builder builder) {
      this.builder = builder;
   }

   @Override
   public int getStatus() {
      return builder.getStatus();
   }

   @Override
   public Object getEntity() {
      return builder.getEntity();
   }

   public static class Builder implements RestResponseBuilder<Builder> {

      private Map<String, Object> headers = new HashMap<>();
      private int status;
      private Object entity;
      private CacheControl cacheControl;
      private MediaType type;
      private Date expires;
      private Date lastModified;
      private String tag;

      @Override
      public SimpleRestResponse build() {
         return new SimpleRestResponse(this);
      }

      @Override
      public Builder status(int status) {
         this.status = status;
         return this;
      }

      @Override
      public Builder entity(Object entity) {
         this.entity = entity;
         return this;
      }

      @Override
      public Builder cacheControl(CacheControl cacheControl) {
         this.cacheControl = cacheControl;
         return this;
      }

      @Override
      public Builder header(String name, Object value) {
         headers.put(name, value);
         return this;
      }

      @Override
      public Builder contentType(MediaType type) {
         this.type = type;
         return this;
      }

      @Override
      public Builder expires(Date expires) {
         this.expires = expires;
         return this;
      }

      @Override
      public Builder lastModified(Date lastModified) {
         this.lastModified = lastModified;
         return this;
      }

      @Override
      public Builder eTag(String tag) {
         this.tag = tag;
         return this;
      }

      @Override
      public int getStatus() {
         return status;
      }

      @Override
      public Object getEntity() {
         return entity;
      }

      @Override
      public Object getHeader(String header) {
         return headers.get(header);
      }
   }
}
