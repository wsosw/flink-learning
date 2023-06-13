package com.blackpearl.gmall.function;

import com.alibaba.fastjson.JSONObject;

public interface DimAsyncJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject dimInfo);

}
