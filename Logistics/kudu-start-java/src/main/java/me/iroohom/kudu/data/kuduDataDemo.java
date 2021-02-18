package me.iroohom.kudu.data;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;

/**
 * @ClassName: kuduTableDemo
 * @Author: Roohom
 * @Function: 基于Java API对kudu进行CURD操作
 * @Date: 2020/12/8 14:47
 * @Software: IntelliJ IDEA
 */
public class kuduDataDemo {
    private KuduClient kuduClient = null;

    /**
     * 用于构建Kudu表中每列的字段信息Schema
     *
     * @param name  字段名称
     * @param type  字段类型
     * @param isKey 是否为Key
     * @return ColumnSchema对象
     */
    private ColumnSchema newColunmnSchema(String name, Type type, boolean isKey) {
        // 创建ColumnSchemaBuilder实例对象
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        // 设置是否为主键
        column.key(isKey);
        // 构建 ColumnSchema
        return column.build();
    }

    /**
     * 建立连接
     */
    @Before
    public void init() {
        kuduClient = new KuduClient.KuduClientBuilder("node2:7051")
                .defaultSocketReadTimeoutMs(6000)
                .build();
    }

    @Test
    public void insertKudu() throws KuduException {
        //获取会话封装会话
        KuduSession kuduSession = kuduClient.newSession();
        //传递表的名称获取表的句柄
        KuduTable kuduTable = kuduClient.openTable("users");
        //获取insert对象设置row
        Insert insert = kuduTable.newInsert();
        PartialRow insertRow = insert.getRow();
        insertRow.addInt("id", 1001);
        insertRow.addString("name", "jack");
        insertRow.addByte("age", (byte) 23);

        //执行插入
        OperationResponse response = kuduSession.apply(insert);
        System.out.println(response.getElapsedMillis());

        //关闭session
        kuduSession.close();
    }

    @Test
    public void insertKuduBatch() throws KuduException {
        Random random = new Random();

        //获取会话封装会话
        KuduSession kuduSession = kuduClient.newSession();
        //手动提交刷写
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(3000);
        //传递表的名称获取表的句柄
        KuduTable kuduTable = kuduClient.openTable("users");
        for (int i = 0; i < 100; i++) {
            //获取insert对象设置row
            Insert insert = kuduTable.newInsert();
            PartialRow insertRow = insert.getRow();
            insertRow.addInt("id", 1001 + i);
            insertRow.addString("name", "jack" + i);
            insertRow.addByte("age", (byte) (18 + random.nextInt(10)));
            //执行插入
            OperationResponse response = kuduSession.apply(insert);
        }

        //手动刷写
        kuduSession.flush();

        //关闭session
        kuduSession.close();
    }

    /**
     * 扫描全表数据并打印
     *
     * @throws KuduException 异常
     */
    @Test
    public void selectKuduData() throws KuduException {
        KuduTable kuduTable = kuduClient.openTable("unit_stu_info_new_3");
        //查询数据，扫描全表，查询所有数据
        KuduScanner.KuduScannerBuilder kuduScannerBuilder = kuduClient.newScannerBuilder(kuduTable);
        //扫描出数据
        KuduScanner kuduScanner = kuduScannerBuilder.build();
        //从扫描器中获取数据
        //判断是否还有下个分区数据未读取
        int partition = 1;
        while (kuduScanner.hasMoreRows()) {
            System.out.println("Current partition is partition" + (partition++));
            //获取每个分区的数据
            RowResultIterator rowResults = kuduScanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult rowResult = rowResults.next();
                int id = rowResult.getInt("id");
                String name = rowResult.getString("name");
//                byte age = rowResult.getByte("age");
                System.out.println("id = " + id + ", name = " + name);
            }
        }
    }

    /**
     * 查询Kudu表数据
     *
     * @throws KuduException 异常
     */
    @Test
    public void queryKuduData() throws KuduException {
        KuduTable kuduTable = kuduClient.openTable("users");
        //查询数据，扫描全表，查询所有数据
        KuduScanner.KuduScannerBuilder kuduScannerBuilder = kuduClient.newScannerBuilder(kuduTable);

        //设置筛选条件
        ArrayList<String> columnNames = new ArrayList<String>();
        columnNames.add("id");
//        columnNames.add("name");
        columnNames.add("name");
        kuduScannerBuilder.setProjectedColumnNames(columnNames);

        KuduPredicate kuduPredicateId = KuduPredicate.newComparisonPredicate(
                newColunmnSchema("id", Type.INT32, true),
                KuduPredicate.ComparisonOp.GREATER,
                1020
        );
        KuduPredicate kuduPredicateAge = KuduPredicate.newComparisonPredicate(
                newColunmnSchema("name", Type.INT8, false),
                KuduPredicate.ComparisonOp.LESS,
                (byte) 23
        );
        kuduScannerBuilder.addPredicate(kuduPredicateId)
                .addPredicate(kuduPredicateAge);


        //扫描出数据
        KuduScanner kuduScanner = kuduScannerBuilder.build();
        //从扫描器中获取数据
        //判断是否还有下个分区数据未读取
        while (kuduScanner.hasMoreRows()) {
            //获取每个分区的数据
            RowResultIterator rowResults = kuduScanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult rowResult = rowResults.next();
                int id = rowResult.getInt("id");
                String name = rowResult.getString("name");
                byte age = rowResult.getByte("age");
                System.out.println("id = " + id + ", name = " + name);
            }
        }
    }

    /**
     * 对Kudu中表的数据进行插入更新，如果主键不存在插入数据，主键存在就更新数据
     */
    @Test
    public void upsertKuduTableData() throws KuduException {
        KuduTable kuduTable = kuduClient.openTable("users");

        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        Upsert upsert = kuduTable.newUpsert();
        PartialRow upsertRow = upsert.getRow();

        upsertRow.addInt("id", 1001);
        upsertRow.addString("name", "Mike");
        upsertRow.addByte("age", (byte) 23);

        //执行插入更新
        kuduSession.apply(upsert);
        kuduSession.flush();

        //关闭会话
        kuduSession.close();
    }


    /**
     * 删除Kudu表的数据
     */
    @Test
    public void deleteKuduData() throws KuduException {

        KuduTable kuduTable = kuduClient.openTable("users");

        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        //创建删除对象
        Delete delete = kuduTable.newDelete();
        PartialRow deleteRow = delete.getRow();
        deleteRow.addInt("id", 1002);
        deleteRow.addByte("age", (byte) 20);

        //执行删除
        kuduSession.apply(delete);
        kuduSession.flush();

        //关闭会话
        kuduSession.close();
    }


    /**
     * 测试完关闭连接
     *
     * @throws KuduException 异常
     */
    @After
    public void clean() throws KuduException {
        if (kuduClient != null) {
            kuduClient.close();
        }
    }

}
