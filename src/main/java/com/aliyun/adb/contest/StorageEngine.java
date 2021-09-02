package com.aliyun.adb.contest;
import java.util.concurrent.locks.ReentrantLock;

import static com.aliyun.adb.contest.VmingAnalyticalDB.partition;
import static com.aliyun.adb.contest.VmingAnalyticalDB.threadNum;
import static com.aliyun.adb.contest.VmingAnalyticalDB.topK;
public class StorageEngine {

    private Bucket[] buckets;
    int count = 3_0000_0000;//3_0000_0000;10000

    public StorageEngine(String filePrefix){
        buckets = new Bucket[partition];
        for(int i=0;i<partition;++i){
            buckets[i] = new Bucket(filePrefix + "_" + i);
        }
    }
    public void add(long val) throws Exception {
        int index = getBucketIndex(val);
        buckets[index].add(val);
    }
    public void flush() throws Exception {
        for(int i=0;i<partition;++i){
            buckets[i].persist();
        }

    }
    private int getBucketIndex(long val){
        return (int)(val >>> (64-topK));
    }

    public long quantile(double percentile){
        int pos = (int)Math.round(percentile * count);
        int index = 0;
        long sum =0;
        for(;index<partition;++index){
            sum+=buckets[index].count;
            if(sum>=pos)
                break;
        }
        if(index==0)sum=0;
        else sum -= buckets[index].count;
        return buckets[index].quantile(pos-sum-1);
    }
}
