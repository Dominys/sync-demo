package com.sync.demo.syncdemo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@AllArgsConstructor
public class DownloadResponse {
    private String jobId;
    private String id;
    private Status status;
    private Object data;
}
