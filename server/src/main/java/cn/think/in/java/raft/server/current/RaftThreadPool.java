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
package cn.think.in.java.raft.server.current;

import java.util.concurrent.*;

/**
 * RaftThreadPool 是一个用于 Raft 服务的线程池工具类，提供了线程池管理、
 * 定时任务调度以及线程池任务执行的功能。
 * <p>
 * 该类封装了线程池的初始化和管理逻辑，简化了 Raft 服务中多线程任务的使用。
 *
 * @author 莫那·鲁道
 */
public class RaftThreadPool {

    // 获取当前系统的可用CPU核心数
    private static final int CUP = Runtime.getRuntime().availableProcessors();

    // 最大线程池大小 = CPU核心数的2倍
    private static final int MAX_POOL_SIZE = CUP * 2;

    // 阻塞队列的大小
    private static final int QUEUE_SIZE = 1024;

    // 线程空闲时间（单位：毫秒），超过此时间的空闲线程将被回收
    private static final long KEEP_TIME = 1000 * 60;

    // 线程空闲时间的时间单位
    private static final TimeUnit KEEP_TIME_UNIT = TimeUnit.MILLISECONDS;

    // 定时任务线程池，负责执行定时任务
    private static final ScheduledExecutorService ss = getScheduled();

    // 通用线程池，负责普通的并发任务执行
    private static final ThreadPoolExecutor te = getThreadPool();

    /**
     * 初始化通用线程池
     *
     * @return 初始化后的线程池对象
     */
    private static ThreadPoolExecutor getThreadPool() {
        return new RaftThreadPoolExecutor(
                CUP, // 核心线程数
                MAX_POOL_SIZE, // 最大线程数
                KEEP_TIME, // 线程空闲时间
                KEEP_TIME_UNIT, // 时间单位
                new LinkedBlockingQueue<>(QUEUE_SIZE), // 任务队列
                new NameThreadFactory() // 自定义线程工厂
        );
    }

    /**
     * 初始化定时任务线程池
     *
     * @return 初始化后的定时任务线程池对象
     */
    private static ScheduledExecutorService getScheduled() {
        return new ScheduledThreadPoolExecutor(CUP, new NameThreadFactory());
    }

    /**
     * 提交一个定时执行的任务，按照固定速率周期性执行。
     *
     * @param r         任务
     * @param initDelay 初始延迟时间（毫秒）
     * @param delay     每次执行之间的时间间隔（毫秒）
     */
    public static void scheduleAtFixedRate(Runnable r, long initDelay, long delay) {
        ss.scheduleAtFixedRate(r, initDelay, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * 提交一个定时执行的任务，按照固定延迟周期性执行。
     *
     * @param r     任务
     * @param delay 每次执行之间的固定延迟时间（毫秒）
     */
    public static void scheduleWithFixedDelay(Runnable r, long delay) {
        ss.scheduleWithFixedDelay(r, 0, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * 提交一个有返回值的任务到线程池，并返回 Future 对象。
     *
     * @param r   可调用任务
     * @param <T> 返回值类型
     * @return 包含任务执行结果的 Future 对象
     */
    @SuppressWarnings("unchecked")
    public static <T> Future<T> submit(Callable r) {
        return te.submit(r);
    }

    /**
     * 提交一个无返回值的任务到线程池。
     *
     * @param r 可运行任务
     */
    public static void execute(Runnable r) {
        te.execute(r);
    }

    /**
     * 提交一个任务到线程池，并允许选择同步或异步执行。
     *
     * @param r    可运行任务
     * @param sync 是否同步执行
     */
    public static void execute(Runnable r, boolean sync) {
        if (sync) {
            // 同步执行：直接运行任务
            r.run();
        } else {
            // 异步执行：提交到线程池
            te.execute(r);
        }
    }

    /**
     * 自定义线程工厂，用于生成线程。
     * 每个线程都命名为 "Raft thread"，并设置为守护线程。
     */
    static class NameThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new RaftThread("Raft thread", r); // 使用自定义线程类 RaftThread
            t.setDaemon(true); // 设置为守护线程
            t.setPriority(5); // 设置优先级为5（默认值）
            return t;
        }
    }

}
