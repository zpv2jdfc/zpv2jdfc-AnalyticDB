package com.aliyun.adb.contest;

import com.aliyun.adb.contest.spi.AnalyticDB;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;

public class VmingAnalyticalDB implements AnalyticDB {
    //    分桶根据top k位
    public static int topK = 7;
    //    分区数
    public static int partition = 64;
    //    线程数
    public static int threadNum = 16;
//    列数
    private int colNum = 2;

    private StorageEngine[] storageEngines;
    private static final Logger logger = Logger.getLogger(VmingAnalyticalDB.class);
    @Override
    public void load(String tpchDataFileDir, String workspaceDir) throws Exception {
        File file = new File(tpchDataFileDir);
        for(File dataFile : file.listFiles()){
            String tableName = dataFile.getName();
            if(!tableName.equals("lineitem"))
                continue;
            logger.info(tableName);
            storageEngines = new StorageEngine[colNum];
            for(int i=0;i<colNum;++i){
                storageEngines[i] = new StorageEngine(workspaceDir + "/" + i);
            }
//            计算每个分区开始位置
            FileChannel fileChannel = new RandomAccessFile(dataFile, "r").getChannel();
            long[] readPosition = new long[threadNum];
            readPosition[0] = 21;
            long total = fileChannel.size();
            for(int i=1;i<threadNum;++i){
                long startPosition = total / threadNum *i;
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, startPosition, 50);
                for(int j=0;j<50;++j){
                    byte b = mappedByteBuffer.get();
                    if(b=='\n'){
                        startPosition += j+1;
                        break;
                    }
                }
                readPosition[i] = startPosition;
            }
            CountDownLatch countDownLatch = new CountDownLatch(threadNum);
//          读取
            for(int i=0;i<threadNum;++i){
                int finalI = i;

                new Thread(()->{
                    try{
                        long endPos = total-1;
                        if(finalI !=threadNum-1){
                            endPos = readPosition[finalI +1] - 1;
                        }
                        long startPos = readPosition[finalI];
                        long startTime =System.currentTimeMillis();
                        int bufferSize = Math.min(1024*4, (int)(endPos - startPos +1));
                        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
                        while(startPos<endPos){
                            byteBuffer.clear();
                            int size = fileChannel.read(byteBuffer, startPos);
                            size = Math.min(size, (int)(endPos - startPos+1));
                            while(size>0 && byteBuffer.get(size-1)!='\n'){
                                --size;
                            }
                            for(int j=0;j<size;){
                                byte b = byteBuffer.get(j);
                                long val = 0;
                                while(b!=','){
                                    val = val * 10 + b - '0';
                                    b = byteBuffer.get(++j);
                                }
                                storageEngines[0].add(val);
                                val = 0;
                                ++j;
                                b = byteBuffer.get(j);
                                while (b!='\n'){
                                    val = val*10 + b-'0';
                                    b = byteBuffer.get(++j);
                                }
                                storageEngines[1].add(val);
                                ++j;
                            }
                            startPos += size;
                        }
                        storageEngines[0].flush();
                        storageEngines[1].flush();
                        countDownLatch.countDown();
                        logger.info("thread:"+Thread.currentThread()+" time cost:"+(System.currentTimeMillis() - startTime));
                    }catch (Exception e){
                        logger.error("thread:"+Thread.currentThread()+ " msg:" + e);
                    }
                }).start();
            }
            countDownLatch.await();
        }
    }

    @Override
    public String quantile(String table, String column, double percentile) throws Exception {
        StorageEngine engine = null;
        if(column.equals("L_ORDERKEY")){
            engine = storageEngines[0];
        }else {
            engine = storageEngines[1];
        }
        long val = engine.quantile(percentile);
        return String.valueOf(val);
    }

    private String bucketName(String tableName, int colIndex){
        return tableName + "_" + colIndex;
    }

}
