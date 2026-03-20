package org.infinispan.rest.framework;

import java.io.Closeable;

public interface FormParts extends Closeable {

   FormPart get(String name);

   int size();

   @Override
   void close();
}
