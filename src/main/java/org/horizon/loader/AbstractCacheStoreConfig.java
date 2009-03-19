package org.horizon.loader;

/**
 * Configures {@link AbstractCacheStore}.  This allows you to tune a number of characteristics of the {@link
 * AbstractCacheStore}.
 * <p/>
 * <ul> <li><tt>purgeSynchronously</tt> - whether {@link org.horizon.loader.CacheStore#purgeExpired()} calls happen
 * synchronously or not.  By default, this is set to <tt>false</tt>.</li>
 * <p/>
 * </ul>
 *
 * @author Mircea.Markus@jboss.com
 * @version $Id: AbstractCacheStoreConfig.java 7852 2009-03-05 00:22:03Z adriancole $
 * @since 1.0
 */
public class AbstractCacheStoreConfig extends AbstractCacheLoaderConfig {

   private boolean purgeSynchronously = false;

   public boolean isPurgeSynchronously() {
      return purgeSynchronously;
   }

   public void setPurgeSynchronously(boolean purgeSynchronously) {
      testImmutability("purgeSynchronously");
      this.purgeSynchronously = purgeSynchronously;
   }
}
