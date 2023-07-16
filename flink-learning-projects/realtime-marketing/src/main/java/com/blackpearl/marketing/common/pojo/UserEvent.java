package com.blackpearl.marketing.common.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserEvent {
    // 用户自增标识
    private int guid;
    private String eventId;
    private Map<String,String> properties;
    private long eventTime;
}
