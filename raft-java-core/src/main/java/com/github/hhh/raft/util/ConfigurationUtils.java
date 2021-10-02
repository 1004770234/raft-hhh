package com.github.hhh.raft.util;

import com.github.hhh.raft.proto.RaftProto;

import java.util.List;


public class ConfigurationUtils {

//     configuration不会太大，所以这里直接遍历了
//1、遍历查找    for (RaftProto.Server server : configuration.getServersList())
    public static boolean containsServer(RaftProto.Configuration configuration, int serverId) {
        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == serverId) {
                return true;
            }
        }
        return false;
    }


//从configuration中删除多个server
// 1、思路
//    ①×：修改传入的对象
//    ②√：根据传入的对象，生成一个【新对象】，直接返回【删除后的新结果】。
//      @@避免了修改对象
    public static RaftProto.Configuration removeServers(
            RaftProto.Configuration configuration, List<RaftProto.Server> servers) {
        RaftProto.Configuration.Builder confBuilder = RaftProto.Configuration.newBuilder();
        for (RaftProto.Server server : configuration.getServersList()) {
            boolean toBeRemoved = false;
            for (RaftProto.Server server1 : servers) {
                if (server.getServerId() == server1.getServerId()) {
                    toBeRemoved = true;
                    break;
                }
            }
            if (!toBeRemoved) {
                confBuilder.addServers(server);
            }
        }
        return confBuilder.build();
    }

//用serverId从configuration中获取server
    public static RaftProto.Server getServer(RaftProto.Configuration configuration, int serverId) {
        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == serverId) {
                return server;
            }
        }
        return null;
    }

}
