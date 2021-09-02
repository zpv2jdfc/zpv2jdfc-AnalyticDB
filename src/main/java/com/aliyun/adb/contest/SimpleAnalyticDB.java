package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.logging.Logger;

public class SimpleAnalyticDB implements AnalyticDB {


    private static Logger log = Logger.getLogger(SimpleAnalyticDB.class.toString());
//    桶的数量
    private int bucketCount = 128;
//    缓存容量上限
    private int limit = 1024*128;
//    1列的缓存
    private Queue<Long>[] que1 = new Queue[bucketCount];
//    2列的缓存
    private Queue<Long>[] que2 = new Queue[bucketCount];
//    数据目录
    private String dataFileDir = "";
//    工作目录
    private String workspaceDir = "";
//    每次读取文件大小
    private int bufferSize = 1024*1024*1024;
//  value右移求对应桶编号
    private int rightShift = -1;
//    每个桶内的元素
    private int[]size1 = null;
    private int[]size2 = null;
//    数据总个数
    private int count = 0;
//    前缀和
    private int[] pre1 = null;
    private int[] pre2 = null;

    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        this.dataFileDir = tpchDataFileDir;
        this.workspaceDir = workspaceDir;
        initBucket();
        for(File file : new File(this.dataFileDir).listFiles()){
            doLoad(file);
        }
        sort();
        for(int i=1;i<bucketCount;++i){
            pre1[i] = size1[i-1] + pre1[i-1];
            pre2[i] = size2[i-1] + pre2[i-1];
        }
        count = pre1[bucketCount-1] + size1[bucketCount-1];
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        log.info("table" + table);
        log.info("column" + column);
        log.info("percentile" + percentile);
        int index = (int)(count * percentile) - 1;
        int col = 1;
        int bucket = -1;
        int num = -1;
        if(column.equals("L_ORDERKEY")){
            int l = 0, r=bucketCount-1, m=-1;
            while(l<=r){
                m = l+r>>>1;
                int v = pre1[m];
                if(v==index){
                    break;
                } else if (v>index) {
                    r = m-1;
                }else {
                    l=m + 1;
                }
            }
            bucket = r;
            num = index - pre1[bucket];
        }else {
            col = 2;
            int l = 0, r=bucketCount-1, m=-1;
            while(l<=r){
                m = l+r>>>1;
                int v = pre2[m];
                if(v==index){
                    break;
                } else if (v>index) {
                    r = m-1;
                }else {
                    l=m + 1;
                }
            }
            bucket = r;
            num = index - pre2[bucket];
        }
        return Long.toString(find(col, bucket, num));
    }
    private long find(int col, int bucket, int index) throws Exception{
        FileChannel file = new RandomAccessFile(workspaceDir + "/" +col+ "_" + bucket,"r").getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        int end = file.read(buffer);
        int pos = index*8;
        long a = 0;
        for(int i=0;i<8;++i){
            a = a<<8;
            a += buffer.get(pos+i) & 0xff;
        }
        file.close();
        return a;
    }
    private void doLoad(File file) throws Exception{
        RandomAccessFile randomAccessFile = new RandomAccessFile(file,"r");
        FileChannel fileChannel = randomAccessFile.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        int r = 0;
        int start = 22;
        long len = file.length();
        int read = 0;
        while(true){
            buffer.clear();
            int end = fileChannel.read(buffer, start);
            while(r+40<end){
                byte temp = 0;
                long a = 0;
                while((temp=buffer.get(r))!=44){
                    a *= 10;
                    a += temp-'0';
                    ++r;
                }
                r += 1;
                long b = 0;
                while((temp=buffer.get(r))!=13){
                    b *= 10;
                    b += temp-'0';
                    ++r;
                }
                save2Cache(a,b);
                r += 2;
            }
            save2File();
            start = r;
            read += r;
            if(read>=len-40)
                break;
        }
        buffer.clear();
        int end = fileChannel.read(buffer, 22 + read);
        r = 0;
        while(r<end){
            long a = 0;
            byte temp = 0;
            while((temp=buffer.get(r))!=44){
                a *= 10;
                a += temp-'0';
                ++r;
            }
            r += 1;
            long b = 0;
            while((temp=buffer.get(r))!=13){
                b *= 10;
                b += temp-'0';
                ++r;
            }
            save2Cache(a,b);
        }
        save2File();
    }
    private void initBucket(){
        for(int i=0;i<bucketCount;++i){
            que1[i] = new LinkedList();
            que2[i] = new LinkedList();
            String firstFileName = workspaceDir + "/1_" + i;
            String secondFileName = workspaceDir + "/2_" + i;
            try {
                File file = new File(firstFileName);
                file.createNewFile();
                file = new File(secondFileName);
                file.createNewFile();
            }catch (Exception e){
                log.severe("can not create file");
                log.severe(e.getMessage());
            }
        }
        int temp = bucketCount;
        rightShift = 1;
        while((temp&(temp-1))!=0){
            temp &= temp-1;
        }
        while((temp=temp>>>1)!=0){
            ++rightShift;
        }
        rightShift = 64 - rightShift;
        log.info("rightShift = "+rightShift);
        size1 = new int[bucketCount];
        size2 = new int[bucketCount];
        pre1 = new int[bucketCount];
        pre2 = new int[bucketCount];
    }
    private void save2Cache(long v1, long v2){
        que1[(int)(v1>>>rightShift)].offer(v1);
        que2[(int)(v2>>>rightShift)].offer(v2);
    }
    private void save2File() throws Exception {
//        ByteBuffer buffer = ByteBuffer.allocate(8);
        for(int i=0;i<bucketCount;++i){
            RandomAccessFile file = new RandomAccessFile(workspaceDir + "/1_" + i,"rw");
            ByteBuffer buffer = ByteBuffer.allocate((que1[i].size())*8);
            FileChannel channel = file.getChannel();
            while(!que1[i].isEmpty()){
                long v = que1[i].poll();
                for(int k=7;k>=0;--k){
                    buffer.put((byte)(v>>>(k*8)));
                }
            }
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
            channel.close();
//
            file = new RandomAccessFile(workspaceDir + "/2_" + i,"rw");
            buffer = ByteBuffer.allocate(que2[i].size()*8);
            channel = file.getChannel();
            while(!que2[i].isEmpty()){
                long v = que2[i].poll();
                for(int k=7;k>=0;--k){
                    buffer.put((byte)(v>>>(k*8)));
                }

            }
            buffer.flip();
            channel.write(buffer);
            buffer.clear();
            channel.close();
        }
    }
    private void sort() throws Exception{
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        for(int i=0;i<bucketCount;++i){
            Queue<Long>que = new PriorityQueue<>();
            FileChannel file = new RandomAccessFile(workspaceDir + "/1_" + i,"r").getChannel();
            buffer.clear();
            int end = file.read(buffer);
            int pos = 0;
            while(pos<end){
                long a = 0;
                for(int k=0;k<8;++k){
                    a = a<<8;
                    a |= buffer.get(k+pos) & 0xff;
                }
                que.offer(a);
                ++size1[i];
                pos += 8;
            }
            que1[i] = que;
            file.close();

            que = new PriorityQueue<>();
            buffer.clear();
            file = new RandomAccessFile(workspaceDir + "/2_" + i,"r").getChannel();
            end = file.read(buffer);
            pos = 0;
            while(pos<end){
                long a = 0;
                for(int k=0;k<8;++k){
                    a = a<<8;
                    a += buffer.get(k+pos) & 0xff;
                }
                que.offer(a);
                ++size2[i];
                pos += 8;
            }
            que2[i] = que;
            file.close();
            save2File(i);
        }
    }
    private void save2File(int bucket) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        new File(workspaceDir + "/1_" + bucket).delete();
        FileChannel file = new RandomAccessFile(workspaceDir + "/1_" + bucket,"rw").getChannel();
        while(!que1[bucket].isEmpty()){
            long v = que1[bucket].poll();
            for(int k=7;k>=0;--k){
                buffer.put((byte)(v>>>(k*8)));
            }
            buffer.flip();
            file.write(buffer);
            buffer.clear();
        }
        file.close();
        new File(workspaceDir + "/2_" + bucket).delete();
        file = new RandomAccessFile(workspaceDir + "/2_" + bucket,"rw").getChannel();
        while(!que2[bucket].isEmpty()){
            long v = que2[bucket].poll();
            for(int k=7;k>=0;--k){
                buffer.put((byte)(v>>>(k*8)));
            }
            buffer.flip();
            file.write(buffer);
            buffer.clear();
        }
        file.close();

    }
}
