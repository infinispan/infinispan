package org.infinispan.commons.util.concurrent.jdk8backported;

class LRUNode<K, V> implements EvictionEntry<K, V> {

    private final BoundedEquivalentConcurrentHashMapV8.Node<K, V> attachedNode;
    StrippedConcurrentLinkedDeque.DequeNode<BoundedEquivalentConcurrentHashMapV8.Node<K, V>> queueNode;
    boolean removed;

    public LRUNode(BoundedEquivalentConcurrentHashMapV8.Node<K, V> item) {
        this.attachedNode = item;
    }

    @Override
    public K getKey() {
        return attachedNode.key;
    }
}
