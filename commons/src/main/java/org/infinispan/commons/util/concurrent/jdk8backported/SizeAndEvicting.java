package org.infinispan.commons.util.concurrent.jdk8backported;

final class SizeAndEvicting {
    final long size;
    final long evicting;

    public SizeAndEvicting(long size, long evicting) {
        this.size = size;
        this.evicting = evicting;
    }

    @Override
    public String toString() {
        return "SizeAndEvicting [size=" + size + ", evicting=" + evicting + "]";
    }
}
