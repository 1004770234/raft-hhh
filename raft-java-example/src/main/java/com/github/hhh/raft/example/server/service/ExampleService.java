package com.github.hhh.raft.example.server.service;

/**
 * Created by hhh on 2017/5/9.
 */
public interface ExampleService {

    ExampleProto.SetResponse set(ExampleProto.SetRequest request);

    ExampleProto.GetResponse get(ExampleProto.GetRequest request);
}
