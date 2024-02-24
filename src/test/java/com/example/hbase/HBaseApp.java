package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseApp {
    Connection connection = null;
    Table table = null;
    Admin admin = null;
    String tableName = "pk_hbase_java_api";

    @Before
    /*Setup Configuration and build connection*/
    public void setUp() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "localhost:2181");
        ConnectionFactory.createConnection();
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            Assert.assertNotNull(connection);
            Assert.assertNotNull(admin);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getConnection() {

    }

    @Test
    /*Create Table*/
    public void createTable() throws Exception{
        TableName table = TableName.valueOf(tableName);
        if(admin.tableExists(table)) {
            System.out.println(tableName + " already exists");
        } else {
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table);
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("info"));
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of("address"));

            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            admin.createTable(tableDescriptor);

            System.out.println(tableName + " created successfully");
        }
    }

    @Test
    /*Get All Table Info*/
    public void queryTableInfo() throws Exception {
        List<TableDescriptor> tables = admin.listTableDescriptors();
        if (tables.size() > 0) {
            for (TableDescriptor table : tables) {
                System.out.println(table.getTableName());

                ColumnFamilyDescriptor[] columnDescriptors = table.getColumnFamilies();
                for (ColumnFamilyDescriptor columnDescriptor : columnDescriptors) {
                    System.out.println("\t" + columnDescriptor.getNameAsString());
                }
            }
        }

    }

    @Test
    /*Put Value in table*/
    public void testPut() throws Exception {
        table = connection.getTable(TableName.valueOf(tableName));

        List<Put> puts = new ArrayList<>();

        Put put1 = new Put(Bytes.toBytes("jepson"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("18"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("xxxx"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("company"), Bytes.toBytes("apple"));
        put1.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("CN"));
        put1.addColumn(Bytes.toBytes("address"), Bytes.toBytes("province"), Bytes.toBytes("SHANGHAI"));
        put1.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("SHANGHAI"));

        Put put2 = new Put(Bytes.toBytes("xx"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("19"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("xxxx"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("company"), Bytes.toBytes("PDD"));
        put2.addColumn(Bytes.toBytes("address"), Bytes.toBytes("country"), Bytes.toBytes("CN"));
        put2.addColumn(Bytes.toBytes("address"), Bytes.toBytes("province"), Bytes.toBytes("SHANGHAI"));
        put2.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("SHANGHAI"));

        puts.add(put1);
        puts.add(put2);

        table.put(puts);
    }

    @Test
    /*Update column*/
    public void testUpdate() throws Exception {
        table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes("xx"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
        table.put(put);
    }

    @Test
    /*Get info*/
    public void testGet() throws Exception {
        table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get("xx".getBytes());
        Result result = table.get(get);
        printResult(result);
    }

    @Test
    /**/
    public void testScan() throws Exception {
        table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("company"));
        //Scan scan = new Scan(new Get(Bytes.toBytes("jepson")));
        ResultScanner rs = table.getScanner(scan);

        for(Result result : rs) {
            printResult(result);
        }
    }

    @Test
    public void testFilter() throws Exception {
        table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ONE);;
        Filter filter1 = new PrefixFilter(Bytes.toBytes("p"));
        Filter filter2 = new PrefixFilter(Bytes.toBytes("j"));

        filters.addFilter(filter1);
        filters.addFilter(filter2);

        filters.addFilter(filter1);
        filters.addFilter(filter2);
        scan.setFilter(filters);

        ResultScanner rs = table.getScanner(scan);
        for(Result result : rs) {
            printResult(result);
            System.out.println("-----------");
        }
    }
    private void printResult(Result result) {
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(result.getRow()) + "\t "
                    + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t"
                    + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t"
                    + Bytes.toString(CellUtil.cloneValue(cell)) + "\t"
                    + cell.getTimestamp());
        }
    }

    @After
    public void tearDown() {
        try{
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
