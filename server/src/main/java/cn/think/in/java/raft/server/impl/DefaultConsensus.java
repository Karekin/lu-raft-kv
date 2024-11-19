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
package cn.think.in.java.raft.server.impl;

import cn.think.in.java.raft.server.Consensus;
import cn.think.in.java.raft.common.entity.LogEntry;
import cn.think.in.java.raft.common.entity.NodeStatus;
import cn.think.in.java.raft.common.entity.Peer;
import cn.think.in.java.raft.common.entity.AentryParam;
import cn.think.in.java.raft.common.entity.AentryResult;
import cn.think.in.java.raft.common.entity.RvoteParam;
import cn.think.in.java.raft.common.entity.RvoteResult;
import io.netty.util.internal.StringUtil;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * 默认的一致性模块实现.
 *
 * @author 莫那·鲁道
 */
@Setter
@Getter
public class DefaultConsensus implements Consensus {


    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConsensus.class);


    public final DefaultNode node;

    public final ReentrantLock voteLock = new ReentrantLock();
    public final ReentrantLock appendLock = new ReentrantLock();

    public DefaultConsensus(DefaultNode node) {
        this.node = node;
    }

    /**
     * 请求投票 RPC
     *
     * 接收者实现：
     *      如果term < currentTerm返回 false （5.2 节）
     *      如果 votedFor 为空或者就是 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
     */
    @Override
    public RvoteResult requestVote(RvoteParam param) {
        try {
            RvoteResult.Builder builder = RvoteResult.newBuilder();
            if (!voteLock.tryLock()) {
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }

            // 对方任期没有自己新
            if (param.getTerm() < node.getCurrentTerm()) {
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }

            // (当前节点并没有投票 或者 已经投票过了且是对方节点) && 对方日志和自己一样新
            LOGGER.info("node {} current vote for [{}], param candidateId : {}", node.peerSet.getSelf(), node.getVotedFor(), param.getCandidateId());
            LOGGER.info("node {} current term {}, peer term : {}", node.peerSet.getSelf(), node.getCurrentTerm(), param.getTerm());

            if ((StringUtil.isNullOrEmpty(node.getVotedFor()) || node.getVotedFor().equals(param.getCandidateId()))) {

                if (node.getLogModule().getLast() != null) {
                    // 对方没有自己新
                    if (node.getLogModule().getLast().getTerm() > param.getLastLogTerm()) {
                        return RvoteResult.fail();
                    }
                    // 对方没有自己新
                    if (node.getLogModule().getLastIndex() > param.getLastLogIndex()) {
                        return RvoteResult.fail();
                    }
                }

                // 切换状态
                node.status = NodeStatus.FOLLOWER;
                // 更新
                node.peerSet.setLeader(new Peer(param.getCandidateId()));
                node.setCurrentTerm(param.getTerm());
                node.setVotedFor(param.getServerId());
                // 返回成功
                return builder.term(node.currentTerm).voteGranted(true).build();
            }

            return builder.term(node.currentTerm).voteGranted(false).build();

        } finally {
            voteLock.unlock();
        }
    }


    /**
     * 附加日志(多个日志,为了提高效率) RPC
     *
     * 接收者实现：
     *    如果 term < currentTerm 就返回 false （5.1 节）
     *    如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false （5.3 节）
     *    如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 （5.3 节）
     *    附加任何在已有的日志中不存在的条目
     *    如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
     */
    @Override
    public AentryResult appendEntries(AentryParam param) {
        AentryResult result = AentryResult.fail();
        try {
            // 尝试获取日志追加的锁，避免并发冲突
            if (!appendLock.tryLock()) {
                return result; // 如果锁未获取到，返回失败结果
            }

            // 设置返回的当前任期
            result.setTerm(node.getCurrentTerm());

            // 不够格：如果请求中的任期小于当前节点的任期，则拒绝
            if (param.getTerm() < node.getCurrentTerm()) {
                return result;
            }

            // 更新心跳和选举时间，用于维持领导者状态
            node.preHeartBeatTime = System.currentTimeMillis();
            node.preElectionTime = System.currentTimeMillis();

            // 更新领导者信息
            node.peerSet.setLeader(new Peer(param.getLeaderId()));

            // 够格：如果请求的任期大于或等于当前节点的任期，节点降级为 FOLLOWER
            if (param.getTerm() >= node.getCurrentTerm()) {
                LOGGER.debug("node {} become FOLLOWER, currentTerm : {}, param Term : {}, param serverId = {}",
                        node.peerSet.getSelf(), node.currentTerm, param.getTerm(), param.getServerId());
                // 认怂
                node.status = NodeStatus.FOLLOWER;
            }

            // 使用对方的任期更新本节点的任期
            node.setCurrentTerm(param.getTerm());

            // 处理心跳
            if (param.getEntries() == null || param.getEntries().length == 0) {
                LOGGER.info("node {} append heartbeat success , he's term : {}, my term : {}",
                        param.getLeaderId(), param.getTerm(), node.getCurrentTerm());

                // 检查并处理已提交但未应用到状态机的日志
                long nextCommit = node.getCommitIndex() + 1;

                // 更新 commitIndex
                if (param.getLeaderCommit() > node.getCommitIndex()) {
                    int commitIndex = (int) Math.min(param.getLeaderCommit(), node.getLogModule().getLastIndex());
                    node.setCommitIndex(commitIndex);
                    node.setLastApplied(commitIndex);
                }

                // 将日志提交到状态机
                while (nextCommit <= node.getCommitIndex()) {
                    node.stateMachine.apply(node.logModule.read(nextCommit));
                    nextCommit++;
                }

                // 返回成功响应
                return AentryResult.newBuilder().term(node.getCurrentTerm()).success(true).build();
            }

            // 处理日志追加
            // 如果日志存在冲突（索引或任期不匹配），返回失败
            if (node.getLogModule().getLastIndex() != 0 && param.getPrevLogIndex() != 0) {
                LogEntry logEntry;
                if ((logEntry = node.getLogModule().read(param.getPrevLogIndex())) != null) {
                    if (logEntry.getTerm() != param.getPreLogTerm()) {
                        return result; // 日志任期不匹配，返回失败
                    }
                } else {
                    return result; // 日志索引不匹配，返回失败
                }
            }

            // 检查冲突的日志条目，删除冲突的日志及其后的所有日志
            LogEntry existLog = node.getLogModule().read(param.getPrevLogIndex() + 1);
            if (existLog != null && existLog.getTerm() != param.getEntries()[0].getTerm()) {
                node.getLogModule().removeOnStartIndex(param.getPrevLogIndex() + 1); // 删除冲突的日志
            } else if (existLog != null) {
                // 如果日志已存在且没有冲突，不重复写入
                result.setSuccess(true);
                return result;
            }

            // 将新日志写入日志模块
            for (LogEntry entry : param.getEntries()) {
                node.getLogModule().write(entry);
                result.setSuccess(true);
            }

            // 更新日志的提交状态
            long nextCommit = node.getCommitIndex() + 1;

            //如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
            if (param.getLeaderCommit() > node.getCommitIndex()) {
                int commitIndex = (int) Math.min(param.getLeaderCommit(), node.getLogModule().getLastIndex());
                node.setCommitIndex(commitIndex);
                node.setLastApplied(commitIndex);
            }

            // 将日志提交到状态机
            while (nextCommit <= node.getCommitIndex()) {
                node.stateMachine.apply(node.logModule.read(nextCommit));
                nextCommit++;
            }

            // 设置返回的任期
            result.setTerm(node.getCurrentTerm());

            // 确保状态为 FOLLOWER
            node.status = NodeStatus.FOLLOWER;

            // TODO: 是否应当在成功回复之后，才正式提交日志？防止 leader 在等待回复期间宕机。
            return result;

        } finally {
            // 解锁以允许其他日志追加操作
            appendLock.unlock();
        }
    }



}
