package com.myproj.hbase.bo;

import lombok.Builder;
import lombok.Data;

/**
 * @author shenxie
 * @date 2021/1/21
 */
@Data
@Builder
public class AgentInfo {

    private byte[] rowKey;

    private String column;

    private String family;

    private String value;
}
