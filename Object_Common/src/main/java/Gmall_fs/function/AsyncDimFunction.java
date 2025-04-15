package Gmall_fs.function;

/**
 * @基本功能:
 * @program:Gmall_DW
 * @author: B1ue
 * @createTime:2025-04-22 11:10:30
 **/

import Gmall_fs.util.HbaseUtil;
import Gmall_fs.util.RedisUtil;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;


public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements DimFunction<T>{

    private StatefulRedisConnection<String, String> redisAsyncConn;
    private AsyncConnection hBaseAsyncConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConn = HbaseUtil.getHBaseAsyncConnection();

    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
        HbaseUtil.closeAsyncHbaseConnection(hBaseAsyncConn);
    }

    // 异步调用, 流中每来一个元素, 这个方法执行一次
    // 参数 1: 需要异步处理的元素
    // 参数 2: 元素被异步处理完之后, 放入到ResultFuture中,则会输出到后面的流中
    @Override
    public void asyncInvoke(T bean, ResultFuture<T> resultFuture) throws Exception {

//        创建异步编排对象
        CompletableFuture
            .supplyAsync(new Supplier<JSONObject>() {
                @Override
                public JSONObject get() {
                    // 1. 从 redis 读取维度 . 把读取到的维度返回
                    return RedisUtil.readDimAsync(redisAsyncConn, getTableName(), getRowKey(bean));
                }
            })
            .thenApplyAsync(new Function<JSONObject, JSONObject>() {
                @Override
                public JSONObject apply(JSONObject dim) {
                    //2. 当 redis 没有读到维度的时候,  从 hbase 读取维度数据
                    if (dim == null) {
                        // redis 中没有读到
                       dim =  HbaseUtil.readDimAsync(hBaseAsyncConn, "gmall", getTableName(), getRowKey(bean));
                       if (dim != null) {// 把维度写入到 Redis 中
                        RedisUtil.writeDimAsync(redisAsyncConn, getTableName(), getRowKey(bean), dim);
                        System.out.println("未在redis查询到，在 hbase中命中 ");
                       }else {
                           System.out.println("为在redis、hbase中查询到该"+getTableName()+"表的"+getRowKey(bean)+"的数据");
                       }
                    }else{
                        System.out.println("在redis中命中");
                    }

                    return dim;
                }
            })
            .thenAccept(new Consumer<JSONObject>() {
                @Override
                public void accept(JSONObject dim) {
                    // 3. 补充维度到 bean 中.
                    addDims(bean, dim);
                    // 4. 把结果输出
                    resultFuture.complete(Collections.singleton(bean));
                }
            });
    }
}