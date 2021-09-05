package com.jonas.kafka.serializer;

import com.jonas.kafka.pojo.Company;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Protostuff序列化器
 *
 * @author shenjy
 * @version 1.0
 * @date 2021-08-29
 */
public class ProtostuffSerializer implements Serializer<Company> {

    @Override
    public byte[] serialize(String topic, Company data) {
        if (null == data) {
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(data.getClass());
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        byte[] protostuff = null;
        try {
            protostuff = ProtostuffIOUtil.toByteArray(data, schema, buffer);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            buffer.clear();
        }
        return protostuff;
    }
}
