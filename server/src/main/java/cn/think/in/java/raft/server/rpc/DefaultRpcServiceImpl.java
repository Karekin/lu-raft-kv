/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package cn.think.in.java.raft.server.rpc;

import cn.think.in.java.raft.common.entity.ClientKVReq;
import cn.think.in.java.raft.server.changes.ClusterMembershipChanges;
import cn.think.in.java.raft.common.entity.AentryParam;
import cn.think.in.java.raft.common.entity.Peer;
import cn.think.in.java.raft.common.entity.RvoteParam;
import cn.think.in.java.raft.server.impl.DefaultNode;
import cn.think.in.java.raft.common.rpc.Request;
import cn.think.in.java.raft.common.rpc.Response;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.RpcServer;
import lombok.extern.slf4j.Slf4j;
/**
 * follower 为什么能持续接收到 leader 的心跳？
 * ---
 * 一、 关键机制：Raft 心跳处理
 * 1. 长连接的建立
 *    - 在 DefaultRpcServiceImpl 中，通过 RpcServer 实例启动了一个服务端监听器：
 *      rpcServer = new RpcServer(port, false, false);
 *    - 这个 RpcServer 绑定了一个用户处理器 RaftUserProcessor<Request>，用于处理来自客户端或其他节点的请求。
 *    - RaftUserProcessor 实现了 handleRequest 方法，将所有的请求交给 handlerRequest 方法处理。
 * 2. 心跳请求的处理
 *    - 心跳在 Raft 中是通过 AppendEntries 请求（Request.A_ENTRIES）实现的。
 *    - 当 leader 发送心跳时，DefaultRpcServiceImpl 中的 handlerRequest 方法会匹配到 Request.A_ENTRIES 并调用 node.handlerAppendEntries：
 *    - handlerAppendEntries 是 DefaultNode 中的一个方法，负责处理来自 leader 的心跳和日志复制请求。
 * 3. 心跳的定时发送
 *    - leader 节点会周期性地发送心跳，这通过 HeartBeatTask 的定时任务实现。
 *    - 每个心跳都会包含一些基础信息，例如 term（任期）、leaderId（领导者 ID）、commitIndex（已提交日志索引）。
 *    - 这些心跳会通过长连接传输给 follower 节点。
 * 4. 心跳的接收和处理
 *    - follower 节点通过 RpcServer 的长连接持续监听来自 leader 的心跳请求。
 *    - 每当接收到一个心跳，follower 会更新自己的状态，包括：
 *      - 检查领导者的合法性（比较 term 是否大于当前任期）。
 *      - 更新 commitIndex 和日志同步状态。
 * 5. 长连接保持心跳流畅
 *    - RpcServer 在内部维护了连接的生命周期，确保在连接期间能够持续接收来自 leader 的请求。
 *    - 心跳以 Request.A_ENTRIES 请求的形式流入 follower，而 follower 对请求的处理是非阻塞的。
 * ---
 * 二、 为何长连接可以持续接收心跳
 * 1. Raft 协议中的心跳是周期性的
 *    - leader 会以固定时间间隔（如 150ms）发送心跳包。
 *    - leader 使用异步的方式调用 AppendEntries RPC，并通过底层网络协议（如 Netty）发送请求。
 * 2. RPC 的长连接特性
 *    - RpcServer 使用长连接来保持节点间的通信。这避免了每次心跳发送都重新建立连接的开销。
 * 3. 线程模型的支持
 *    - RpcServer 会为每个请求分配线程去处理：
 *      rpcServer.registerUserProcessor(new RaftUserProcessor<Request>() {
 *          @Override
 *          public Object handleRequest(BizContext bizCtx, Request request) {
 *              return handlerRequest(request);
 *          }
 *      });
 *    - 这里的 RaftUserProcessor 使用线程池处理请求，保证了即使心跳频繁到达，也不会阻塞其他任务。
 * 4. 心跳的简单性
 *    - 心跳包通常不携带日志数据，仅包含基础元信息（如任期、领导者 ID 等）。
 *    - 处理心跳的开销较低，follower 只需要解析请求并更新状态。
 * ---
 * 三、 总结：follower 如何通过长连接接收心跳
 * - 连接建立：RpcServer 在 follower 节点上运行，监听来自 leader 的 RPC 请求（包括心跳）。
 * - 心跳发送：leader 节点周期性发送 AppendEntries 请求作为心跳包。
 * - 请求处理：follower 节点通过 DefaultRpcServiceImpl.handlerRequest 方法分发请求，并调用具体的 handlerAppendEntries 方法更新状态。
 * - 长连接维持：RPC 长连接由底层框架（如 Bolt 或 Netty）管理，确保在连接期间可以多次通信。
 * 通过这样的设计，follower 能够在一个长连接中持续接收并处理来自 leader 的心跳包，维持 Raft 协议的正常运行。
 */

/**
 * Raft Server
 *
 * @author 莫那·鲁道
 */
@Slf4j
public class DefaultRpcServiceImpl implements RpcService {

    private final DefaultNode node;

    private final RpcServer rpcServer;

    public DefaultRpcServiceImpl(int port, DefaultNode node) {
        rpcServer = new RpcServer(port, false, false);
        rpcServer.registerUserProcessor(new RaftUserProcessor<Request>() {

            @Override
            public Object handleRequest(BizContext bizCtx, Request request) {
                return handlerRequest(request);
            }
        });

        this.node = node;
    }


    @Override
    public Response<?> handlerRequest(Request request) {
        if (request.getCmd() == Request.R_VOTE) {
            return new Response<>(node.handlerRequestVote((RvoteParam) request.getObj()));
        } else if (request.getCmd() == Request.A_ENTRIES) {
            return new Response<>(node.handlerAppendEntries((AentryParam) request.getObj()));
        } else if (request.getCmd() == Request.CLIENT_REQ) {
            return new Response<>(node.handlerClientRequest((ClientKVReq) request.getObj()));
        } else if (request.getCmd() == Request.CHANGE_CONFIG_REMOVE) {
            return new Response<>(((ClusterMembershipChanges) node).removePeer((Peer) request.getObj()));
        } else if (request.getCmd() == Request.CHANGE_CONFIG_ADD) {
            return new Response<>(((ClusterMembershipChanges) node).addPeer((Peer) request.getObj()));
        }
        return null;
    }


    @Override
    public void init() {
        rpcServer.start();
    }

    @Override
    public void destroy() {
        rpcServer.stop();
        log.info("destroy success");
    }
}
