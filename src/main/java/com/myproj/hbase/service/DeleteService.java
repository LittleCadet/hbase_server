package com.myproj.hbase.service;

import com.myproj.hbase.bo.AgentInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import java.io.IOException;
import java.util.List;
import java.util.Objects;


/**
 * 背景： 应用异常停止 告警不准确
 * 原因： agentEvent的应用异常停止告警：由于hbase中的有脏数据：导致原逻辑： appName去ApplicationIndex表中查找agentId,在根据agentId去AgentInfo表查找agentInfo信息。但是原来的
 * 容器的agentId被改动过，导致hbase中有脏数据。从而导致告警不准确
 *
 * 备注：
 * 运行程序后，最好使用：flush + major_compact 持久化并合并hdfs的dataFile
 *
 * @author shenxie
 * @date 2021/1/21
 */
@Service
public class DeleteService implements InitializingBean {

    private static final String TABLE_NAME = "AgentInfo";

    private static final Long ASSIGN_TIME_START = 1262275200000L;

//    private static final Long ASSIGN_TIME_END = 1611210300000L;
    private static final Long ASSIGN_TIME_END = 1609430400000L;

//    private static final String ZK_ADDRESS = "10.0.0.103";
    private static final String ZK_ADDRESS = "10.0.102.38";

    private static final String PORT = "2181";

    @Override
    public void afterPropertiesSet() throws Exception {
        this.deleted();
    }

    private void deleted() throws IOException {
        List<Delete> deletes = Lists.newArrayList();
        List<AgentInfo> agents = Lists.newArrayList();
        Table table = null;
        ResultScanner scanner = null;

        try{
            //建立连接：
            Configuration configuration = HBaseConfiguration.create();
            configuration.set(HConstants.ZOOKEEPER_QUORUM, ZK_ADDRESS);
            configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, PORT);
            Connection connection = ConnectionFactory.createConnection(configuration);

            // 按照时间戳检索出数据
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Scan scan = new Scan();

            scan.setTimeRange(ASSIGN_TIME_START ,ASSIGN_TIME_END);
            scan.addFamily(Bytes.toBytes("Info"));

            scanner = table.getScanner(scan);

            for(Result result : scanner) {
                agents.addAll(mapRow(result));
            }

            System.out.println("===========找到可删除的数据：【" + agents.size() + "】条====\r\n" + agents);

            agents.forEach(agentInfo -> {
                Delete delete = new Delete(agentInfo.getRowKey());
                delete.addColumn(Bytes.toBytes(agentInfo.getFamily()),Bytes.toBytes(agentInfo.getColumn()));
                deletes.add(delete);
            });

            // 删除
            if( ! CollectionUtils.isEmpty(deletes)) {
                int total = deletes.size();
                table.delete(deletes);
                System.out.println("==========共删除数据：【" + total + "】条");
            } else {
                System.out.println("==========没有需要删除的数据==========");
            }
        }catch (Exception e){
            System.out.println("===================异常:" + e);
        }finally {
            Objects.requireNonNull(scanner).close();
        }
    }

    private List<AgentInfo> mapRow(Result result){

        List<AgentInfo> agents = Lists.newArrayList();
        byte[] rowKey = result.getRow();
        final String[] family = {null};
        final String[] column = {null};
        final String[] value = {null};
        List<Cell> cells = result.listCells();

        cells.forEach(cell -> {
            family[0] = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            column[0] = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            value[0] = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            agents.add(AgentInfo.builder().rowKey(rowKey).column(column[0]).family(family[0]).value(value[0]).build());
        });
        return agents;
    }

}
