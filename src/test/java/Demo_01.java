/**
 * Created by HQ.XPS15
 * on 2018/6/3  17:12
 */

import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.k3a.consistenthash.ConsistentHash;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by HQ.heqing
 * on 2018/5/24  下午5:05
 */
public class Demo_01 {

    /**
     * 测试 kafka 的partition
     */
    public static void main(String[] args) {

        Random random = new Random();
        List<PartitionInfo> collect = Stream.iterate(0, e -> e + 1).limit(20)
                .map(e -> new PartitionInfo(random.nextGaussian() + "", e, new Node(e + 1, e + 2 + "", e + 3), null, null))
                .collect(Collectors.toList());


        int partitionSize = 0;

        ConsistentHash<Object, PartitionInfo, Integer> hash = new ConsistentHash<>(collect, 3, e -> Math.abs(Integer.reverse(e.hashCode())), e -> Math.abs(Integer.reverse(e.hashCode())), (t, u) -> new PartitionInfo(t.topic(), u, null, null, null));


        PartitionInfo partitionInfo = hash.get(100);
        PartitionInfo partitionInfo1 = hash.get(200);
        PartitionInfo partitionInfo2 = hash.get(300);
        PartitionInfo partitionInfo3 = hash.get(400);
        PartitionInfo partitionInfo4 = hash.get(500);
        PartitionInfo partitionInfo5 = hash.get(600);
        PartitionInfo partitionInfo6 = hash.get(700);
        PartitionInfo partitionInfo7 = hash.get(2200);
        PartitionInfo partitionInfo8 = hash.get(2300);
        PartitionInfo partitionInfo9 = hash.get(2000000);
        PartitionInfo partitionInfo10 = hash.get(27);


        hash.add(new PartitionInfo(null, 21, null, null, null));
        hash.add(new PartitionInfo(null, 22, null, null, null));
        hash.add(new PartitionInfo(null, 23, null, null, null));
        hash.add(new PartitionInfo(null, 24, null, null, null));
        hash.add(new PartitionInfo(null, 100, null, null, null));


//        hash.remove(0);
//        hash.remove(1);
//        hash.remove(2);
//        hash.remove(3);
//        hash.remove(4);
        PartitionInfo partitionInfo_ = hash.get(100);
        PartitionInfo partitionInfo1_ = hash.get(200);
        PartitionInfo partitionInfo2_ = hash.get(300);
        PartitionInfo partitionInfo3_ = hash.get(400);
        PartitionInfo partitionInfo4_ = hash.get(500);
        PartitionInfo partitionInfo5_ = hash.get(600);
        PartitionInfo partitionInfo6_ = hash.get(700);
        PartitionInfo partitionInfo7_ = hash.get(22);
        PartitionInfo partitionInfo8_ = hash.get(23);
        PartitionInfo partitionInfo9_ = hash.get(24);
        PartitionInfo partitionInfo10_ = hash.get(27);

    }

}