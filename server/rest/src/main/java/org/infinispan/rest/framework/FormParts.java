package org.infinispan.rest.framework;

import java.io.Closeable;
import java.util.Collection;

public interface FormParts extends Closeable {

   FormPart get(String name);

   Collection<FormPart> parts();

   int size();

   @Override
   void close();
}
