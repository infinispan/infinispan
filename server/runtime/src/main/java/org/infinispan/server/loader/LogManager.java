package org.infinispan.server.loader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * @since 14.0
 **/
public class LogManager extends java.util.logging.LogManager {
   private java.util.logging.LogManager delegate;

   public LogManager() {
      super();
   }

   public synchronized void setDelegate(java.util.logging.LogManager delegate) {
      this.delegate = delegate;
      super.reset();
   }

   @Override
   public boolean addLogger(Logger logger) {
      if (delegate != null) {
         return delegate.addLogger(logger);
      } else {
         return super.addLogger(logger);
      }
   }

   @Override
   public Logger getLogger(String name) {
      if (delegate != null) {
         return delegate.getLogger(name);
      } else {
         return super.getLogger(name);
      }
   }

   @Override
   public Enumeration<String> getLoggerNames() {
      if (delegate != null) {
         return delegate.getLoggerNames();
      } else {
         return super.getLoggerNames();
      }
   }

   @Override
   public void readConfiguration() throws IOException, SecurityException {
      if (delegate != null) {
         delegate.readConfiguration();
      } else {
         super.readConfiguration();
      }
   }

   @Override
   public void reset() throws SecurityException {
      if (delegate != null) {
         delegate.reset();
      } else {
         super.reset();
      }
   }

   @Override
   public void readConfiguration(InputStream ins) throws IOException, SecurityException {
      if (delegate != null) {
         delegate.readConfiguration(ins);
      } else {
         super.readConfiguration(ins);
      }
   }

   @Override
   public void updateConfiguration(Function<String, BiFunction<String, String, String>> mapper) throws IOException {
      if (delegate != null) {
         delegate.updateConfiguration(mapper);
      } else {
         super.updateConfiguration(mapper);
      }
   }

   @Override
   public void updateConfiguration(InputStream ins, Function<String, BiFunction<String, String, String>> mapper) throws IOException {
      if (delegate != null) {
         delegate.updateConfiguration(ins, mapper);
      } else {
         super.updateConfiguration(ins, mapper);
      }
   }

   @Override
   public String getProperty(String name) {
      if (delegate != null) {
         return delegate.getProperty(name);
      } else {
         return super.getProperty(name);
      }
   }

   @Override
   public java.util.logging.LogManager addConfigurationListener(Runnable listener) {
      if (delegate != null) {
         return delegate.addConfigurationListener(listener);
      } else {
         return super.addConfigurationListener(listener);
      }
   }

   @Override
   public void removeConfigurationListener(Runnable listener) {
      if (delegate != null) {
         delegate.removeConfigurationListener(listener);
      } else {
         super.removeConfigurationListener(listener);
      }
   }
}
