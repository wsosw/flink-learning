package com.blackpearl.examples;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public   class EventLog {
    private long guid;
    private String sessionId;
    private String eventId;
    private long timeStamp;
    private Map<String,String> eventInfo;
}
