package com.careerdrill;


import com.careerdrill.entity.Employee;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetMain {
    public static void main(String[] args)  {
        Schema avroSchema = Employee.getClassSchema();

        Employee e1= EmployeeBuilder.buildEmployee();
        int blockSize = 1024;
        int pageSize = 65535;


        try(
                AvroParquetWriter<Employee> parquetWriter = new AvroParquetWriter<Employee>(
                        new org.apache.hadoop.fs.Path("./paraquet-gen/employee.parquet"),
                        avroSchema,
                        CompressionCodecName.SNAPPY,
                        blockSize,
                        pageSize)
        ){

                parquetWriter.write(e1);

        }catch(java.io.IOException e){
            e.printStackTrace();
        }

    }
}
