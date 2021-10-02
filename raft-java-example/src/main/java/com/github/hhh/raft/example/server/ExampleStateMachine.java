package com.github.hhh.raft.example.server;

import com.github.hhh.raft.StateMachine;
import com.github.hhh.raft.example.server.service.ExampleProto;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


public class ExampleStateMachine implements StateMachine {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleStateMachine.class);

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB db;
    private String raftDataDir;

    public ExampleStateMachine(String raftDataDir) {
        this.raftDataDir = raftDataDir;
    }

//1、创建数据库：Checkpoint checkpoint = Checkpoint.create(db);
//2、数据库绑定文件：checkpoint.createCheckpoint(snapshotDir)
    @Override
    public void writeSnapshot(String snapshotDir) {
        Checkpoint checkpoint = Checkpoint.create(db);
        try {
            checkpoint.createCheckpoint(snapshotDir);
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.warn("writeSnapshot meet exception, dir={}, msg={}",
                    snapshotDir, ex.getMessage());
        }
    }


//1、File.separator获取文件分隔符
//2、独特文件："rocksdb_data"
//  ①删除旧的快照文件：FileUtils.deleteDirectory(dataFile);
//  ②新快照文件——指定位置：FileUtils.copyDirectory(snapshotFile, dataFile);
//3、使用rocksDB创建数据库：db = RocksDB.open(options, dataDir);
//4、源文件+目标文件：   FileUtils.copyDirectory(snapshotFile, dataFile);
//          https://www.cnblogs.com/greywolf/p/3209786.html
//5、rocksDB：使用 raftDataDir：dataDir+"rocksdb_data"
//6、把快照文件【复制到】rocksDB数据文件：
//      FileUtils.copyDirectory(snapshotFile, dataFile);
//7、用rocksDB创建数据库：
//      db = RocksDB.open(options, dataDir);

    @Override
    public void readSnapshot(String snapshotDir) {
        try {
            // 关闭之前的db——因为这里需要重新创建
            if (db != null) {
                db.close();
            }
//            如果旧的快照文件存在，就删除
            String dataDir = raftDataDir + File.separator + "rocksdb_data";
            File dataFile = new File(dataDir);
            if (dataFile.exists()) {
                FileUtils.deleteDirectory(dataFile);
            }

//            把新快照文件，复制到指定位置
            File snapshotFile = new File(snapshotDir);
            if (snapshotFile.exists()) {
                FileUtils.copyDirectory(snapshotFile, dataFile);
            }
            // open rocksdb data dir
            Options options = new Options();
            options.setCreateIfMissing(true);
            db = RocksDB.open(options, dataDir);
        } catch (Exception ex) {
            LOG.warn("meet exception, msg={}", ex.getMessage());
        }
    }


// 1、此处为protobuf的编码方式。字段名前自动加【set+get】。parseFrom是从二进制中读取
//     ① bool ParseFromString(const string& data)
//     ② bool ParseFromArray(const char* buf,int size)
// 2、放入rocksDB数据库：
//    db.put(request.getKey().getBytes(), request.getValue().getBytes());
    @Override
    public void apply(byte[] dataBytes) {
        try {
            ExampleProto.SetRequest request = ExampleProto.SetRequest.parseFrom(dataBytes);
            db.put(request.getKey().getBytes(), request.getValue().getBytes());
        } catch (Exception ex) {
            LOG.warn("meet exception, msg={}", ex.getMessage());
        }
    }


//1、builder工厂模式创建
//2、从数据库中获取数据：byte[] valueBytes = db.get(keyBytes);
//3、把二进制转字符串：String value = new String(valueBytes);
    public ExampleProto.GetResponse get(ExampleProto.GetRequest request) {
        try {
            ExampleProto.GetResponse.Builder responseBuilder = ExampleProto.GetResponse.newBuilder();
            byte[] keyBytes = request.getKey().getBytes();
            byte[] valueBytes = db.get(keyBytes);
            if (valueBytes != null) {
                String value = new String(valueBytes);
                responseBuilder.setValue(value);
            }
            ExampleProto.GetResponse response = responseBuilder.build();
            return response;
        } catch (RocksDBException ex) {
            LOG.warn("read rockdb error, msg={}", ex.getMessage());
            return null;
        }
    }

}
