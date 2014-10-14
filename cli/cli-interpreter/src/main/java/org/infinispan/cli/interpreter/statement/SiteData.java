package org.infinispan.cli.interpreter.statement;

public class SiteData {
   final String cacheName;
   final String siteName;

   public SiteData(final String cacheName, final String siteName) {
      this.cacheName = cacheName;
      this.siteName = siteName;
   }

   public SiteData(final String siteName) {
      this(null, siteName);
   }

   public String getCacheName() {
      return cacheName;
   }

   public String getSiteName() {
      return siteName;
   }

}
