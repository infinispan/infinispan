package org.infinispan.commons.util;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;

import org.infinispan.commons.util.FileLookupFactory.DefaultFileLookup;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleReference;

public class OsgiFileLookup extends DefaultFileLookup {
   
   protected OsgiFileLookup() {
   }
   
   protected Collection<URL> getAsURLsFromClassLoader(String filename, ClassLoader userClassLoader) throws IOException {    
      Collection<URL> urls = super.getAsURLsFromClassLoader(filename, userClassLoader);
      // scan osgi bundles
      BundleContext bc = ((BundleReference) FileLookup.class.getClassLoader()).getBundle().getBundleContext();
      for (Bundle bundle : bc.getBundles()) {
         urls.add(bundle.getResource(filename));
      }
      return urls;
   }

}