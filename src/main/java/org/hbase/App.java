package org.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class App {
    private static final String TABLE_NAME = "students";
    private static final String INFO_COLUMN = "info";
    private static final String GRADES_COLUMN = "grades";

    public static void main(String[] args) throws IOException {
        {
            // HBase Configuration
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "zookeeper");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("hbase.master", "hbase-master:16000");

            try (Connection connection = ConnectionFactory.createConnection(config);
                 Admin admin = connection.getAdmin()) {

                TableName tableName = TableName.valueOf(TABLE_NAME);

                if (!admin.tableExists(tableName)) {
                    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(INFO_COLUMN))
                            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(GRADES_COLUMN))
                            .build();

                    admin.createTable(tableDescriptor);
                    System.out.println("Table created");
                }

                try (Table table = connection.getTable(tableName)) {
                    // Insert students
                    Put put1 = new Put(Bytes.toBytes("student1"));
                    put1.addColumn(Bytes.toBytes(INFO_COLUMN), Bytes.toBytes("name"), Bytes.toBytes("John Doe"));
                    put1.addColumn(Bytes.toBytes(INFO_COLUMN), Bytes.toBytes("age"), Bytes.toBytes("20"));
                    put1.addColumn(Bytes.toBytes(GRADES_COLUMN), Bytes.toBytes("math"), Bytes.toBytes("B"));
                    put1.addColumn(Bytes.toBytes(GRADES_COLUMN), Bytes.toBytes("science"), Bytes.toBytes("A"));
                    table.put(put1);

                    Put put2 = new Put(Bytes.toBytes("student2"));
                    put2.addColumn(Bytes.toBytes(INFO_COLUMN), Bytes.toBytes("name"), Bytes.toBytes("Jane Smith"));
                    put2.addColumn(Bytes.toBytes(INFO_COLUMN), Bytes.toBytes("age"), Bytes.toBytes("22"));
                    put2.addColumn(Bytes.toBytes(GRADES_COLUMN), Bytes.toBytes("math"), Bytes.toBytes("A"));
                    put2.addColumn(Bytes.toBytes(GRADES_COLUMN), Bytes.toBytes("science"), Bytes.toBytes("A"));
                    table.put(put2);

                    System.out.println("Students added");

                    // Retrieve student information
                    Get get = new Get(Bytes.toBytes("student1"));
                    Result result = table.get(get);
                    System.out.println("Retrieved student1 info: " + result);

                    // Update Jane Smith's age and math grade
                    Put putUpdate = new Put(Bytes.toBytes("student2"));
                    putUpdate.addColumn(Bytes.toBytes(INFO_COLUMN), Bytes.toBytes("age"), Bytes.toBytes("23"));
                    putUpdate.addColumn(Bytes.toBytes(GRADES_COLUMN), Bytes.toBytes("math"), Bytes.toBytes("A+"));
                    table.put(putUpdate);

                    System.out.println("Updated student2 info");

                    // Delete student1
                    Delete delete = new Delete(Bytes.toBytes("student1"));
                    table.delete(delete);
                    System.out.println("Deleted student1");

                    // Scan and display all students
                    Scan scan = new Scan();
                    try (ResultScanner scanner = table.getScanner(scan)) {
                        for (Result scanResult : scanner) {
                            System.out.println("Found row: " + scanResult);
                        }
                    }
                }
            }
        }
    }
}