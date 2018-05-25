package com.sync.demo.rest.demorest;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class DownloadRequest {
    private String jobId;
    private String token;
    private List<String> ids;
}
