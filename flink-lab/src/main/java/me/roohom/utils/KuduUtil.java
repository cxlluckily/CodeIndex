package me.roohom.utils;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.util.DecimalUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * kudu util
 * @author WY
 */
public enum KuduUtil {

    /**
     * 实例
     */
    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(KuduUtil.class);

    private KuduClient client;

    /**
     * 获取 KuduClient 对象
     */
    private KuduClient getClient(String master) {
        if (client == null) {
            client = new KuduClient.KuduClientBuilder(master).build();
        }
        return client;
    }

    /**
     * 建表
     * @param fields 字段列表 "field1 type1 1,field2 type2 0"
     */
    public void createTable(String master, String table, String fields) {
        KuduClient client = getClient(master);
        try {
            if (!client.tableExists(table)) {
                List<ColumnSchema> columns = new ArrayList<>();
                List<String> rangeKeys = new ArrayList<>();
                String[] info = fields.split(",");
                for (String col : info) {
                    String[] colInfo = col.split(" ");
                    String colName = colInfo[0];
                    String colType = colInfo[1];
                    int isPrimary = Integer.parseInt(colInfo[2]);
                    int typeLength = Integer.parseInt(colInfo[3]);
                    int typeScale = Integer.parseInt(colInfo[4]);
                    columns.add(new ColumnSchema.ColumnSchemaBuilder(colName, Type.getTypeForName(colType.toLowerCase()))
                            .key(isPrimary == 1)
                            .nullable(isPrimary != 1)
                            //decimal类型需要设置该项
                            .typeAttributes("decimal".equals(colType.toLowerCase()) ? DecimalUtil.typeAttributes(typeLength, typeScale) : null)
                            .build());
                    if (isPrimary == 1) {
                        rangeKeys.add(colName);
                    }
                }
                CreateTableOptions cto = new CreateTableOptions();
                cto.addHashPartitions(rangeKeys, 3);
                cto.setNumReplicas(3);
                client.createTable(table, new Schema(columns), cto);
            }
        } catch (KuduException e) {
            LOG.error("create kudu table failed, table: {}, err: {}", table, e);
        }
    }

    /**
     * 删表
     */
    public void deleteTable(String master, String table) {
        KuduClient client = getClient(master);
        try {
            if (client.tableExists(table)) {
                client.deleteTable(table);
            }
        } catch (KuduException e) {
            LOG.error("delete kudu table failed, table: {}, err: {}", table, e);
        }
    }

    /**
     * list 表
     */
    public List<String> listTable(String master) {
        KuduClient client = getClient(master);
        try {
            ListTablesResponse res = client.getTablesList();
            return res.getTablesList();
        } catch (KuduException e) {
            LOG.error("list kudu table failed", e);
        }
        return null;
    }

    /**
     * query 表
     */
    public void query(String master, String table, String... cols) {
        KuduClient client = getClient(master);
        try {
            KuduScanner.KuduScannerBuilder kuduScannerBuilder = client.newScannerBuilder(client.openTable(table));
            kuduScannerBuilder.setProjectedColumnNames(Arrays.asList(cols));
            KuduScanner kuduScanner = kuduScannerBuilder.build();

            while (kuduScanner.hasMoreRows()){
                RowResultIterator rowResults = kuduScanner.nextRows();
                while(rowResults.hasNext()){
                    RowResult row = rowResults.next();
                    for (String col : cols) {
                        System.out.println(col + ": " + row.getObject(col) + " " + row.getColumnType(col));
                    }
                    System.out.println("-----------------------------");
                }
            }
        } catch (KuduException e) {
            LOG.error("query kudu table failed", e);
        }
    }
}
