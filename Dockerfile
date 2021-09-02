FROM registry.cn-shanghai.aliyuncs.com/tcc-public/maven:sql  
WORKDIR /2021-tianchi-contest-1-master  
RUN rm -rf /2021-tianchi-contest-1-master/*  
COPY src /2021-tianchi-contest-1-master/src  
COPY pom.xml /2021-tianchi-contest-1-master  
COPY target/adb-contest-1.0-SNAPSHOT.jar /2021-tianchi-contest-1-master/target/adb-contest-1.0-SNAPSHOT.jar