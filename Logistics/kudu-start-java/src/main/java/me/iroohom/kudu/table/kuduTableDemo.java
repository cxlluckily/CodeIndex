package me.iroohom.kudu.table;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

/**
 * @ClassName: kuduTableDemo
 * @Author: Roohom
 * @Function: 基于Java API对kudu进行CURD操作
 * @Date: 2020/12/8 14:47
 * @Software: IntelliJ IDEA
 */
public class kuduTableDemo {
    private KuduClient kuduClient = null;

    /**
     * 建立连接
     */
    @Before
    public void init() {
        kuduClient = new KuduClient.KuduClientBuilder("node2:7051")
                .defaultSocketReadTimeoutMs(6000)
                .build();
    }

    /**
     * 用于构建一个Kudu表的Schema对象
     *
     * @param name  表名
     * @param type  类型
     * @param isKey 是否是主键
     * @return 新的列约束
     */
    private ColumnSchema newColumnSchema(String name, Type type, boolean isKey) {
        //创建列Schema构造器对象
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(isKey);

        return column.build();
    }


    /**
     * 创建Kudu的表
     *
     * @throws KuduException 异常
     */
    @Test
    public void testCreateKuduTable() throws KuduException {
        //定义Schema
        ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).key(false).build());
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("age", Type.INT8).key(false).build());
        Schema schema = new Schema(columnSchemas);

        CreateTableOptions builder = new CreateTableOptions();

        ArrayList<String> cols = new ArrayList<String>();
        cols.add("id");
        builder.addHashPartitions(cols, 3);
        //设置副本
        builder.setNumReplicas(1);

        KuduTable kuduTable = kuduClient.createTable("users", schema, builder);
        System.out.println("Kudu Table ID = " + kuduTable.getTableId());
    }

    /**
     * 创建Kudu表根据范围分区
     */
    @Test
    public void createKuduTableRange() throws KuduException {
        //定义Schema信息
        ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
        columnSchemas.add(newColumnSchema("name", Type.STRING, false));
        columnSchemas.add(newColumnSchema("age", Type.INT8, false));
        Schema schema = new Schema(columnSchemas);

        CreateTableOptions tableOptions = new CreateTableOptions();

        /**
         * 设置分区策略，分区数目
         * VITAL: 设置按照哪些字段进行范围分区，字段必须是主键或者是主键部分字段
         */
        ArrayList<String> cols = new ArrayList<String>();
        cols.add("id");
        tableOptions.setRangePartitionColumns(cols);
        //再设置每个分区的范围
        //id < 100
        PartialRow lower = new PartialRow(schema);
        PartialRow upper100 = new PartialRow(schema);
        upper100.addInt("id", 100);
        tableOptions.addRangePartition(lower, upper100);

        //id 在100 到 500 之间
        PartialRow lower100 = new PartialRow(schema);
        lower100.addInt("id", 100);
        PartialRow upper500 = new PartialRow(schema);
        upper500.addInt("id", 500);
        tableOptions.addRangePartition(lower100, upper500);

        //id 大于500
        PartialRow lower500 = new PartialRow(schema);
        lower500.addInt("id", 500);
        PartialRow upperMore = new PartialRow(schema);
        tableOptions.addRangePartition(lower500, upperMore);

        //设置副本数量为1
        tableOptions.setNumReplicas(1);

        //创建表
        KuduTable kuduTable = kuduClient.createTable("users", schema, tableOptions);
        System.out.println("Kudu Table ID = " + kuduTable.getTableId());
    }

    /**
     * 创建Kudu表，设置分区方式为：多级分区，先哈希分区，再范围分区
     */
    @Test
    public void createKuduTableMulti() throws KuduException {
        ArrayList<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        columnSchemas.add(newColumnSchema("id", Type.INT32, true));
        columnSchemas.add(newColumnSchema("age", Type.INT8, true));
        columnSchemas.add(newColumnSchema("name", Type.STRING, false));

        Schema schema = new Schema(columnSchemas);
        /**
         * 设置Kudu属性配置
         */
        CreateTableOptions tableOptions = new CreateTableOptions();

        //设置分区策略分区数目
        //设置Hash分区，分区数目为5 VITAL: 注意此处的写法
        tableOptions.addHashPartitions(
                new ArrayList<String>() {
                    {
                        add("id");
                    }

                },
                5
        );

        //按照age划分范围分区
        tableOptions.setRangePartitionColumns(
                new ArrayList<String>() {
                    {
                        add("age");
                    }
                }
        );

        //年龄<=20
        PartialRow lower = new PartialRow(schema);
        PartialRow upper21 = new PartialRow(schema);
        upper21.addByte("age", (byte) 21);
        tableOptions.addRangePartition(lower, upper21);
        //年龄在21到40之间
        PartialRow lower21 = new PartialRow(schema);
        lower21.addByte("age", (byte) 21);
        PartialRow upper41 = new PartialRow(schema);
        upper41.addByte("age", (byte) 41);
        tableOptions.addRangePartition(lower21, upper41);
        //年龄在41以上
        PartialRow lower41 = new PartialRow(schema);
        lower41.addByte("age", (byte) 41);
        PartialRow upperMore = new PartialRow(schema);
        tableOptions.addRangePartition(lower41, upperMore);


        //设置副本数量
        tableOptions.setNumReplicas(1);

        KuduTable kuduTable = kuduClient.createTable("users", schema, tableOptions);
        System.out.println(kuduTable.getTableId());
    }

    /**
     * 增加列
     *
     * @throws KuduException 异常
     */
    @Test
    public void alterKuduTableAddColumn() throws KuduException {
        AlterTableOptions tableOptions = new AlterTableOptions();
        tableOptions.addColumn("gender", Type.BOOL,true);

        AlterTableResponse response = kuduClient.alterTable("users", tableOptions);
        System.out.println(response.getTableId());
    }




    /**
     * 删除Kudu的表
     *
     * @throws KuduException 异常
     */
    @Test
    public void deleteKuduTable() throws KuduException {
        //判断是否先存在
        if (kuduClient.tableExists("users")) {
            DeleteTableResponse response = kuduClient.deleteTable("users");
            System.out.println(response.getElapsedMillis());
        }
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
