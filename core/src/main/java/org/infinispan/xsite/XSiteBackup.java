package org.infinispan.xsite;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class XSiteBackup {
   private String siteName;
   private boolean sync;
   private long timeout;


   public XSiteBackup(String siteName, boolean sync, long timeout) {
      this.siteName = siteName;
      this.sync = sync;
      this.timeout = timeout;
   }

   public String getSiteName() {
      return siteName;
   }

   public boolean isSync() {
      return sync;
   }

   public long getTimeout() {
      return timeout;
   }

   public String toString() {
      return siteName + " (" + (sync? "sync" : "async") + ", timeout=" + timeout + ")";
   }
}
