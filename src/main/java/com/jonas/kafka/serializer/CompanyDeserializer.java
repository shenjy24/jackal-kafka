package com.jonas.kafka.serializer;

import com.jonas.kafka.company.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 自定义反序列化器
 *
 * @author shenjy
 * @version 1.0
 * @date 2021-09-05
 */
public class CompanyDeserializer implements Deserializer<Company> {

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (null == data) {
            return null;
        }
        if (8 > data.length) {
            throw new SerializationException("Size of data received by CompanyDeserializer is shorter than expected!");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        //解析name
        int nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        //解析address
        int addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        String name = new String(nameBytes, StandardCharsets.UTF_8);
        String address = new String(addressBytes, StandardCharsets.UTF_8);
        return new Company(name, address);
    }
}
