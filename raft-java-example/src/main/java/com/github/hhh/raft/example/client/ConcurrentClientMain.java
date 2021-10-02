package com.github.hhh.raft.example.client;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.github.hhh.raft.example.server.service.ExampleProto;
import com.github.hhh.raft.example.server.service.ExampleService;
import com.googlecode.protobuf.format.JsonFormat;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class ConcurrentClientMain {
    private static JsonFormat jsonFormat = new JsonFormat();

//    1、注意：这里的参数THREAD_NUM——
//          用于RpcClient rpcClient = new RpcClient(ipPorts);
//    2、写入线程+读取线程
//          ExecutorService readThreadPool = Executors.newFixedThreadPool(3);
//          ExecutorService writeThreadPool = Executors.newFixedThreadPool(3);
//    3、三个写入任务：
//          for (int i = 0; i < 3; i++) {
//            future[i] = writeThreadPool.submit(new SetTask(exampleService, readThreadPool));
//        }
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.printf("Usage: ./run_concurrent_client.sh THREAD_NUM\n");
            System.exit(-1);
        }

        // parse args
        String ipPorts = args[0];
        RpcClient rpcClient = new RpcClient(ipPorts);
        ExampleService exampleService = BrpcProxy.getProxy(rpcClient, ExampleService.class);

        ExecutorService readThreadPool = Executors.newFixedThreadPool(3);
        ExecutorService writeThreadPool = Executors.newFixedThreadPool(3);
        Future<?>[] future = new Future[3];
        for (int i = 0; i < 3; i++) {
            future[i] = writeThreadPool.submit(new SetTask(exampleService, readThreadPool));
        }
    }

//1、有示例的服务：private ExampleService exampleService;
//2、特殊：写入任务，里面有读取线程池的实例
//      ExecutorService readThreadPool;
//3、一个持续运行的线程： while (true)
//4、调用set服务
//      ExampleProto.SetResponse setResponse = exampleService.set(setRequest);
//5、如果写入成功，再进行一个【读取任务】
//      readThreadPool.submit(new GetTask(exampleService, key));
//
    public static class SetTask implements Runnable {
        private ExampleService exampleService;
        ExecutorService readThreadPool;

        public SetTask(ExampleService exampleService, ExecutorService readThreadPool) {
            this.exampleService = exampleService;
            this.readThreadPool = readThreadPool;
        }

        @Override
        public void run() {
            while (true) {
                String key = UUID.randomUUID().toString();
                String value = UUID.randomUUID().toString();
                ExampleProto.SetRequest setRequest = ExampleProto.SetRequest.newBuilder()
                        .setKey(key).setValue(value).build();

                long startTime = System.currentTimeMillis();
                ExampleProto.SetResponse setResponse = exampleService.set(setRequest);
                try {
                    if (setResponse != null) {
                        System.out.printf("set request, key=%s, value=%s, response=%s, elapseMS=%d\n",
                                key, value, jsonFormat.printToString(setResponse), System.currentTimeMillis() - startTime);
                        readThreadPool.submit(new GetTask(exampleService, key));
                    } else {
                        System.out.printf("set request failed, key=%s value=%s\n", key, value);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }


//1、读取数据：用例子的实例
// ExampleProto.GetResponse getResponse = exampleService.get(getRequest);
//
//
//
//
//
    public static class GetTask implements Runnable {
        private ExampleService exampleService;
        private String key;

        public GetTask(ExampleService exampleService, String key) {
            this.exampleService = exampleService;
            this.key = key;
        }

        @Override
        public void run() {
            ExampleProto.GetRequest getRequest = ExampleProto.GetRequest.newBuilder()
                    .setKey(key).build();
            long startTime = System.currentTimeMillis();
            ExampleProto.GetResponse getResponse = exampleService.get(getRequest);
            try {
                if (getResponse != null) {
                    System.out.printf("get request, key=%s, response=%s, elapseMS=%d\n",
                            key, jsonFormat.printToString(getResponse), System.currentTimeMillis() - startTime);
                } else {
                    System.out.printf("get request failed, key=%s\n", key);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
