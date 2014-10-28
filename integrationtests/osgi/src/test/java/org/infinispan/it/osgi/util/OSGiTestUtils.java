package org.infinispan.it.osgi.util;

import java.io.File;
import java.net.URL;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;

public class OSGiTestUtils {
   public static BundleContext getBundleContext(Object object) {
      return getBundleContext(object.getClass());
   }

   public static BundleContext getBundleContext(Class<?> clss) {
      if (clss == null) {
         throw new IllegalArgumentException("Class cannot be null.");
      }
      Bundle bundle = FrameworkUtil.getBundle(clss);
      if (bundle == null) {
         throw new IllegalArgumentException(String.format("Failed to find bundle for class '%s'.", clss));
      }
      BundleContext bundleContext = bundle.getBundleContext();
      if (bundleContext == null) {
         throw new IllegalArgumentException(String.format("Failed to retrieve bundle context for class '%s'.", clss));
      }
      return bundleContext;
   }

   public static <S> S getService(BundleContext bundleContext, Class<S> serviceClss) {
      ServiceReference<S> serviceReference = bundleContext.getServiceReference(serviceClss);
      if (serviceReference == null) {
         throw new IllegalArgumentException(String.format("Unable to retrieve service reference for class '%s'.", serviceClss));
      }
      S service = bundleContext.getService(serviceReference);
      if (service == null) {
         throw new IllegalArgumentException(String.format("Unable to retrieve service from reference for class '%s'.", serviceClss));
      }
      return service;
   }

   public static File getResourceFile(String resource) {
      URL url = OSGiTestUtils.class.getClassLoader().getResource(resource);
      if (url == null || !"file".equalsIgnoreCase(url.getProtocol())) {
         throw new IllegalArgumentException("Cannot find file resource: " + resource);
      }
      return new File(url.getFile());
   }
}
