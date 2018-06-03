package org.k3a.consistenthash;

import java.util.Collection;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by HQ.heqing
 * on 2018/6/1  下午4:33
 * 一致性哈希
 */
public class ConsistentHash<K, N, H> {
    //
    private final SortedMap<H, N> continuum = Collections.synchronizedSortedMap(new TreeMap<>());
    //hash 算法
    private final Function<N, H> nodeHash;
    private final Function<K, H> keyHash;
    //创建副本的算法
    private final BiFunction<N, Integer, N> createReplica;
    //使用虚拟节点来平衡
    private final int replicaNum;

    public ConsistentHash(Collection<N> nodes, int replicaNum, Function<N, H> nodeHash, Function<K, H> keyHash, BiFunction<N, Integer, N> createReplica) {
        this.replicaNum = replicaNum;
        this.nodeHash = nodeHash;
        this.keyHash = keyHash;
        this.createReplica = createReplica;
        for (N node : nodes)
            add(node);
    }

    public boolean contains(N node) {
        return continuum.containsValue(node);
    }

    public void add(N node) {
        continuum.put(nodeHash.apply(node), node);
        if (createReplica != null)
            for (int i = 0; i < replicaNum; i++)
                continuum.put(nodeHash.apply(createReplica.apply(node, i)), node);
    }

    public void remove(N node) {
        for (int i = 0; i < replicaNum; i++)
            continuum.remove(nodeHash.apply(createReplica.apply(node, i)));
    }

    public N get(K key) {
        if (continuum.isEmpty())
            return null;
        H hash = keyHash.apply(key);
        if (!continuum.containsKey(hash)) {
            SortedMap<H, N> tailMap = continuum.tailMap(hash);
            hash = tailMap.isEmpty() ? continuum.firstKey() : tailMap.firstKey();
        }
        return continuum.get(hash);
    }

    public SortedMap<H, N> getContinuum() {
        return continuum;
    }

}