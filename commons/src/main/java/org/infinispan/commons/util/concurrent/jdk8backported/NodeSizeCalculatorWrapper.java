package org.infinispan.commons.util.concurrent.jdk8backported;

public final class NodeSizeCalculatorWrapper<K, V> extends AbstractEntrySizeCalculatorHelper<K, V> {
    private final EntrySizeCalculator<? super K, ? super V> calculator;
    private final long nodeAverageSize;

    public NodeSizeCalculatorWrapper(EntrySizeCalculator<? super K, ? super V> calculator) {
        this.calculator = calculator;
        // The node itself is an object and has a reference to its class
        long calculateNodeAverageSize = OBJECT_SIZE + POINTER_SIZE;
        // 6 variables in Node, 5 object references
        calculateNodeAverageSize += 5 * POINTER_SIZE;
        // 1: the int for the hash
        calculateNodeAverageSize += 4;
        // 2: Key actual size is ignored - defined by user
        // 3: NodeEquivalence is ignored as it is shared between all of the nodes
        // 4: Value actual size is ignored - defined by user
        // 5: We have a reference to another node so it is ignored
        // 6: EvictionEntry currently we only support LRU, so assume that node
        long lruNodeSize = calculateLRUNodeSize();
        nodeAverageSize = roundUpToNearest8(calculateNodeAverageSize) + lruNodeSize;
    }

    private long calculateLRUNodeSize() {
        // The lru node itself is an object and has a reference to its class
        long size = OBJECT_SIZE + POINTER_SIZE;
        // LRUNode has 2 object references in it and 1 boolean
        size += 2 * POINTER_SIZE;
        // 1: LRUNode has a pointer back to an internal node, so nothing is added
        // 2: LRUNode has a DequeNode
        long dequeNodeSize = calculateDequeNodeSize();
        // 3: LRUNode has a boolean
        size += 1;
        return roundUpToNearest8(size) + dequeNodeSize;
    }

    private long calculateDequeNodeSize() {
        // Deque node itself is object and has a reference to its class
        long size = OBJECT_SIZE + POINTER_SIZE;
        // Deque node has 3 references in it
        size += 3 * POINTER_SIZE;
        // 2 of the references are other deque nodes (ignored) and the other is pointing
        // back to the node itself (ignored)
        return roundUpToNearest8(size);
    }

    @Override
    public long calculateSize(K key, V value) {
        long result = calculator.calculateSize(key, value) + nodeAverageSize;
        if (result < 0) {
            throw new ArithmeticException("Size overflow!");
        }
        return result;
    }
}
