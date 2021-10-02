package com.github.hhh.raft.admin;

import com.github.hhh.raft.proto.RaftProto;
import com.github.hhh.raft.service.RaftClientService;
import com.googlecode.protobuf.format.JsonFormat;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.List;

//【×】客户端日志操作
//【√】用于【修改服务器数量】


public class AdminMain {
    private static final JsonFormat jsonFormat = new JsonFormat();


//    1、操作实例：【代理类proxy】
//      RaftClientService client = new RaftClientServiceProxy(servers);
//    2、①get:获得配置
//      ②add:增加配置——所以有了args[3]
//      ③del:减少配置
//3、记得关闭：
//  ((RaftClientServiceProxy) client).stop();


    public static void main(String[] args) {
        // parse args
        if (args.length < 3) {
            System.out.println("java -jar AdminMain servers cmd subCmd [args]");
            System.exit(1);
        }
        // servers format is like "10.1.1.1:8010:1,10.2.2.2:8011:2,10.3.3.3.3:8012:3"
        String servers = args[0];
        // conf
        String cmd = args[1];
        //get/add/del
        String subCmd = args[2];

        Validate.isTrue(cmd.equals("conf"));
        Validate.isTrue(subCmd.equals("get")
                || subCmd.equals("add")
                || subCmd.equals("del"));
        RaftClientService client = new RaftClientServiceProxy(servers);

//获得配置
        if (subCmd.equals("get")) {
            RaftProto.GetConfigurationRequest request = RaftProto.GetConfigurationRequest.newBuilder().build();
            RaftProto.GetConfigurationResponse response = client.getConfiguration(request);
            if (response != null) {
                System.out.println(jsonFormat.printToString(response));
            } else {
                System.out.printf("response == null");
            }
//添加配置：所以有了args[3]
        } else if (subCmd.equals("add")) {
            List<RaftProto.Server> serverList = parseServers(args[3]);
            RaftProto.AddPeersRequest request = RaftProto.AddPeersRequest.newBuilder()
                    .addAllServers(serverList).build();
            RaftProto.AddPeersResponse response = client.addPeers(request);
            if (response != null) {
                System.out.println(response.getResCode());
            } else {
                System.out.printf("response == null");
            }
        } else if (subCmd.equals("del")) {
            List<RaftProto.Server> serverList = parseServers(args[3]);
            RaftProto.RemovePeersRequest request = RaftProto.RemovePeersRequest.newBuilder()
                    .addAllServers(serverList).build();
            RaftProto.RemovePeersResponse response = client.removePeers(request);
            if (response != null) {
                System.out.println(response.getResCode());
            } else {
                System.out.printf("response == null");
            }
        }
        ((RaftClientServiceProxy) client).stop();
    }


//   1、 把【字符串】转换为【ArrayList】：
//    分隔符转换
//    ①String[] splitArray1 = serversString.split(",");
//    ②String[] splitArray2 = addr.split(":");
    public static List<RaftProto.Server> parseServers(String serversString) {
        List<RaftProto.Server> serverList = new ArrayList<>();
        String[] splitArray1 = serversString.split(",");
        for (String addr : splitArray1) {
            String[] splitArray2 = addr.split(":");
            RaftProto.Endpoint endPoint = RaftProto.Endpoint.newBuilder()
                    .setHost(splitArray2[0])
                    .setPort(Integer.parseInt(splitArray2[1])).build();
            RaftProto.Server server = RaftProto.Server.newBuilder()
                    .setEndpoint(endPoint)
                    .setServerId(Integer.parseInt(splitArray2[2])).build();
            serverList.add(server);
        }
        return serverList;
    }
}
