package com.github.hhh.raft.example.server.service.impl;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.hhh.raft.Peer;
import com.github.hhh.raft.example.server.ExampleStateMachine;
import com.github.hhh.raft.example.server.service.ExampleProto;
import com.github.hhh.raft.example.server.service.ExampleService;
import com.github.hhh.raft.RaftNode;
import com.github.hhh.raft.proto.RaftProto;
import com.googlecode.protobuf.format.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//1、重要的
//  ①raftNode
//  ②stateMachine
//  ③leaderRpcClient：只是和leader之间的

public class ExampleServiceImpl implements ExampleService {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleServiceImpl.class);
    private static JsonFormat jsonFormat = new JsonFormat();
//jsonFormat.printToString(request)：把对象转换为json对象
    private RaftNode raftNode;
    private ExampleStateMachine stateMachine;
    private int leaderId = -1;
    private RpcClient leaderRpcClient = null;
    private Lock leaderLock = new ReentrantLock();

    public ExampleServiceImpl(RaftNode raftNode, ExampleStateMachine stateMachine) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
    }

//     @@@RpcClient
//1、对象锁：leaderLock.lock();
//2、选择leader的peer，建立rpc连接
//3、RpcClient：leaderRpcClient = new RpcClient(endpoint, rpcClientOptions);
    private void onLeaderChangeEvent() {
        if (raftNode.getLeaderId() != -1
                && raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()
                && leaderId != raftNode.getLeaderId()) {
            leaderLock.lock();
            if (leaderId != -1 && leaderRpcClient != null) {
                leaderRpcClient.stop();
                leaderRpcClient = null;
                leaderId = -1;
            }
            leaderId = raftNode.getLeaderId();
            Peer peer = raftNode.getPeerMap().get(leaderId);
            Endpoint endpoint = new Endpoint(peer.getServer().getEndpoint().getHost(),
                    peer.getServer().getEndpoint().getPort());
            RpcClientOptions rpcClientOptions = new RpcClientOptions();
            rpcClientOptions.setGlobalThreadPoolSharing(true);

            leaderRpcClient = new RpcClient(endpoint, rpcClientOptions);
            leaderLock.unlock();
        }
    }


//1、【√】如果自己不是leader，将写请求转发给leader————【新建rpc对象】
//    onLeaderChangeEvent();
//    ExampleService exampleService = BrpcProxy.getProxy(leaderRpcClient, ExampleService.class);
//    ExampleProto.SetResponse responseFromLeader = exampleService.set(request);
//2、如果自己是leader
//      boolean success = raftNode.replicate(data, RaftProto.EntryType.ENTRY_TYPE_DATA);
//3、mergeFrom：内容合并（两个相同的对象）
//    responseBuilder.mergeFrom(responseFromLeader);
//4、如果

    @Override
    public ExampleProto.SetResponse set(ExampleProto.SetRequest request) {
        ExampleProto.SetResponse.Builder responseBuilder = ExampleProto.SetResponse.newBuilder();
        // 如果自己不是leader，将写请求转发给leader
        if (raftNode.getLeaderId() <= 0) {
            responseBuilder.setSuccess(false);
        }
        else if (raftNode.getLeaderId() != raftNode.getLocalServer().getServerId()) {
            onLeaderChangeEvent();
            ExampleService exampleService = BrpcProxy.getProxy(leaderRpcClient, ExampleService.class);
            ExampleProto.SetResponse responseFromLeader = exampleService.set(request);
//            mergeFrom是什么意思？
            responseBuilder.mergeFrom(responseFromLeader);
        } else {
            // 数据同步写入raft集群
            byte[] data = request.toByteArray();
            boolean success = raftNode.replicate(data, RaftProto.EntryType.ENTRY_TYPE_DATA);
            responseBuilder.setSuccess(success);
        }

        ExampleProto.SetResponse response = responseBuilder.build();
        LOG.info("set request, request={}, response={}", jsonFormat.printToString(request),
                jsonFormat.printToString(response));
        return response;
    }


//    1、读请求：从stateMachine（数据库）
//      stateMachine.get(request);
//    2、本质：从数据库里拿
    @Override
    public ExampleProto.GetResponse get(ExampleProto.GetRequest request) {
        ExampleProto.GetResponse response = stateMachine.get(request);
        LOG.info("get request, request={}, response={}", jsonFormat.printToString(request),
                jsonFormat.printToString(response));
        return response;
    }

}
