package com.aliyun.adb.contest;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Bucket {
    private static final int WRITE_SIZE = 1024 * 4;
    private FileChannel fileChannel;
    private ByteBuffer byteBuffer;
    private int bufferIndex = 0;
    private int maxIndex = 0;

    private int position = 0;

    public int count = 0;

    private Lock lock = new ReentrantLock();

    private static final Logger logger = Logger.getLogger(Bucket.class);
    public Bucket(String fileName){
        File file = new File(fileName);
        try{
            file.createNewFile();
        }catch (Exception e){
            logger.error(e);
        }
        try{
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
            byteBuffer = ByteBuffer.allocate(WRITE_SIZE);
            maxIndex =WRITE_SIZE / 8;
        }catch (Exception e){
            logger.error(e);
        }
    }

    public synchronized void add(long val)throws Exception{
        try{
            ++count;
            byteBuffer.putLong(val);
        }catch (Exception e){
            logger.error(Thread.currentThread() + " " + e);
        }
        ++bufferIndex;
        if(bufferIndex== maxIndex){
            persist();
        }
    }
    public synchronized void persist()throws Exception{
        try{
            byteBuffer.flip();
            fileChannel.write(byteBuffer, position);
            position += bufferIndex * 8;
            byteBuffer.clear();
            bufferIndex = 0;
        }catch (Exception e){
            logger.error(e);
            throw e;
        }
    }

    public long quantile(long pos){
        long[]nums = new long[count];
        try {
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, count*8);
            for(int i=0;i<count;++i){
                long temp = mappedByteBuffer.getLong();
                nums[i] = temp;
            }
        }catch (Exception e){
            logger.error(e);
        }

        return quickSelect(nums, 0,nums.length-1, (int)pos);
    }
    private long quickSelect(long[]nums, int start, int end, int k){
        if(start == end){
            return nums[start];
        }
        int l = start;
        int r = end;
        long p = nums[(start+end)/2];
        swap(nums, start, (start+end)/2);
        while(l<r){
            while (l<r && nums[r]>p)
                --r;
            while(l<r && nums[l]<=p)
                ++l;
            if (l<r){
                swap(nums,l,r);
            }
        }
        swap(nums, start, l);
        if(k==l){
            return nums[l];
        }
        if(k>l){
            return quickSelect(nums,l+1,end,k);
        }else {
            return quickSelect(nums,start,l-1,k);
        }
    }
    private void swap(long[]nums, int a, int b){
        long temp = nums[a];
        nums[a] = nums[b];
        nums[b] =temp;
    }
}
