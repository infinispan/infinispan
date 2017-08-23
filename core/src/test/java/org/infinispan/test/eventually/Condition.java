package org.infinispan.test.eventually;

public interface Condition {
   boolean isSatisfied() throws Exception;
}
