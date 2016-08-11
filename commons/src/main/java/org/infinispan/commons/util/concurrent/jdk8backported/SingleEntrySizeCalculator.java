package org.infinispan.commons.util.concurrent.jdk8backported;

final class SingleEntrySizeCalculator implements EntrySizeCalculator<Object, Object> {
    final static SingleEntrySizeCalculator SINGLETON = new SingleEntrySizeCalculator();

   @Override
    public long calculateSize(Object key, Object value) {
        return 1;
    }
}
