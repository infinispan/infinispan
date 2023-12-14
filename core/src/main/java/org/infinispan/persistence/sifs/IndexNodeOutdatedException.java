package org.infinispan.persistence.sifs;

class IndexNodeOutdatedException extends Exception {
   IndexNodeOutdatedException(String message) {
      super(message);
   }
}
