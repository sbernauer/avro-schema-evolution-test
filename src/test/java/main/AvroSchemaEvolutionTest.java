package main;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.jupiter.api.Test;

public class AvroSchemaEvolutionTest {
  
  @Test
  public void testAvroSchemaEvolution_1() throws IOException {
    Schema schemaA = new Schema.Parser().parse(new File(getClass().getClassLoader().getResource("schemaA.avsc").getFile()));
    Schema schemaB = new Schema.Parser().parse(new File(getClass().getClassLoader().getResource("schemaB.avsc").getFile()));
    File output = new File("output.avro");

    // Write record of A using schema A
    GenericRecord record = new GenericData.Record(schemaA);
    record.put("timestamp", 0d);
    record.put("rider", "myRider0");
    record.put("driver", "myDriver0");
    
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schemaA));
    dataFileWriter.create(schemaA, output);
    dataFileWriter.append(record);
    dataFileWriter.close();
    
    // Write record of B using schema B
    record = new GenericData.Record(schemaB);
    record.put("timestamp", 1d);
    record.put("rider", "myRider1");
    record.put("driver", "myDriver1");
    record.put("evoluted_optional_union_field", "myEvolutedOptionalUnionField1");

    dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schemaB));
    dataFileWriter.appendTo(output);
    dataFileWriter.append(record);
    dataFileWriter.close();

    // Write record of A using schema B
    record = new GenericData.Record(schemaA);
    record.put("timestamp", 2d);
    record.put("rider", "myRider2");
    record.put("driver", "myDriver2");

    dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schemaB));
    dataFileWriter.appendTo(output);
    dataFileWriter.append(record);
    dataFileWriter.close();

    // Read records back
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schemaB);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(output, datumReader);

    record = null;
    long counter = 0;
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next(record);
      assertEquals((double)counter, record.get("timestamp"));
      assertEquals("myRider" + counter, record.get("rider").toString());
      assertEquals("myDriver" + counter, record.get("driver").toString());
      if (counter == 1) {
        assertEquals("{\"type\":\"record\",\"name\":\"triprec\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"double\"},{\"name\":\"rider\",\"type\":\"string\"},{\"name\":\"driver\",\"type\":\"string\"},{\"name\":\"evoluted_optional_union_field\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}", record.getSchema().toString());
        // FAILS WITH expected: <myEvolutedOptionalUnionField1> but was: <null>
        assertEquals("myEvolutedOptionalUnionField" + counter, record.get("evoluted_optional_union_field"));
      }
      counter++;
    }
    dataFileReader.close();
    assertEquals(3, counter);
  }

  @Test
  public void testAvroSchemaEvolution_2() throws IOException {
    Schema schemaA = new Schema.Parser().parse(new File(getClass().getClassLoader().getResource("schemaA.avsc").getFile()));
    Schema schemaB = new Schema.Parser().parse(new File(getClass().getClassLoader().getResource("schemaB.avsc").getFile()));
    File output = new File("output.avro");

    // Write record of B using schema B
    GenericRecord record = new GenericData.Record(schemaB);
    record.put("timestamp", 1d);
    record.put("rider", "myRider1");
    record.put("driver", "myDriver1");
    record.put("evoluted_optional_union_field", "myEvolutedOptionalUnionField1");

    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schemaB));
    dataFileWriter.create(schemaB, output);
    dataFileWriter.append(record);
    dataFileWriter.close();

    // Write record of A using schema B
    record = new GenericData.Record(schemaA);
    record.put("timestamp", 2d);
    record.put("rider", "myRider2");
    record.put("driver", "myDriver2");

    dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schemaB));
    dataFileWriter.appendTo(output);
    dataFileWriter.append(record);
    dataFileWriter.close();
  }
}
