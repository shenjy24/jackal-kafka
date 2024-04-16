package com.jonas.kafka.serializer;

import com.jonas.kafka.company.Company;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 自定义序列化器
 *
 * @author shenjy
 * @version 1.0
 * @date 2021-08-29
 */
public class CompanySerializer implements Serializer<Company> {

    @Override
    public byte[] serialize(String topic, Company data) {
        if (null == data) {
            return null;
        }
        byte[] name, address;
        try {
            if (null != data.getName()) {
                name = data.getName().getBytes(StandardCharsets.UTF_8);
            } else {
                name = new byte[0];
            }
            if (null != data.getAddress()) {
                address = data.getAddress().getBytes(StandardCharsets.UTF_8);
            } else {
                address = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new byte[0];
    }
}
