package org.infinispan.commons.util.concurrent.jdk8backported;

final class LIRSNode<K, V> implements EvictionEntry<K, V> {
    // The next few variables are to always be protected by "this" object monitor
    LIRSEvictionPolicy.Recency state;
    StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> stackNode;
    StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> queueNode;
    boolean created;
    final K key;

    public LIRSNode(K key) {
        this.key = key;
    }

    public void setState(LIRSEvictionPolicy.Recency recency) {
        state = recency;
    }

    public void setStackNode(StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> stackNode) {
        this.stackNode = stackNode;
    }

    public void setQueueNode(StrippedConcurrentLinkedDeque.DequeNode<LIRSNode<K, V>> queueNode) {
        this.queueNode = queueNode;
    }

    @Override
    public String toString() {
        return "LIRSNode [state=" + state + ", stackNode=" +
              System.identityHashCode(stackNode) + ", queueNode=" +
              System.identityHashCode(queueNode)
              + ", key=" + key + "]";
    }

    @Override
    public K getKey() {
        return key;
    }
}
