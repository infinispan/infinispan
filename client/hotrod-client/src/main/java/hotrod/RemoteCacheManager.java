package hotrod;

import hotrod.impl.RemoteCacheSpi;
import org.infinispan.lifecycle.Lifecycle;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
public class RemoteCacheManager implements Lifecycle {

   private Properties props;

   /**
    * Build a cache manager based on supplied given properties.
    * TODO - add a list of all possible configuration parameters here
    */
   public RemoteCacheManager(Properties props) {
      this.props = props;
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties)}, but it will try to lookup the config properties in the
    * classpath, in a file named <tt>hotrod-client.properties</tt>.
    * @throws HotRodClientException if such a file cannot be found in the classpath
    */
   public RemoteCacheManager() {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      InputStream stream = loader.getResourceAsStream("hotrod-client.properties");
      loadFromStream(stream);
   }

   /**
    * Same as {@link #RemoteCacheManager(java.util.Properties)}, but it will try to lookup the config properties in
    * supplied URL.
    * @throws HotRodClientException if properties could not be loaded
    */
   public RemoteCacheManager(URL config) {
      try {
         loadFromStream(config.openStream());
      } catch (IOException e) {
         throw new HotRodClientException("Could not read URL:" + config, e);
      }
   }

   private void loadFromStream(InputStream stream) {
      props = new Properties();
      try {
         props.load(stream);
      } catch (IOException e) {
         throw new HotRodClientException("Issues configuring from client hotrod-client.properties",e);
      }
   }

   public RemoteCacheSpi getRemoteCache(String remoteCacheName) {
      return null;
   }

   public RemoteCacheSpi getDefaultRemoteCache() {
      return null;
   }

   @Override
   public void start() {
      // TODO: Customise this generated block
   }

   @Override
   public void stop() {
      // TODO: Customise this generated block
   }
}
