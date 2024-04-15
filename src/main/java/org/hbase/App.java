package org.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class App {
    private static final String TABLE_NAME = "students";
    private static final String INFO_COLUMN = "info";
    private static final String GRADES_COLUMN = "grades";

    public static void main(String[] args) {
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

            // Insert students
            try (Table table = connection.getTable(tableName)) {
                // Student 1
                Put put1 = new Put(Bytes.toBytes("student1"));
                put1.addColumn(Bytes.toBytes(INFO_COLUMN), Bytes.toBytes("name"), Bytes.toBytes("John Doe"));
                put1.addColumn(Bytes.toBytes(INFO_COLUMN), Bytes.toBytes("age"), Bytes.toBytes("20"));
                put1.addColumn(Bytes.toBytes(GRADES_COLUMN), Bytes.toBytes("math"), Bytes.toBytes("A"));
                table.put(put);
                System.out.println("Data inserted");

                // Retrieve data
                Get get = new Get("1".getBytes());
                Result result = table.get(get);
                System.out.println("Get result: " + result);

                // Delete data
                Delete delete = new Delete("1".getBytes());
                table.delete(delete);
                System.out.println("Data deleted");
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Delete table if it exists
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("Table deleted");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}