package com.jonas.kafka.deserializer;

import com.jonas.kafka.pojo.Company;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Protostuff反序列化器
 *
 * @author shenjy
 * @version 1.0
 * @date 2021-09-05
 */
public class ProtostuffDeserializer implements Deserializer<Company> {

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (null == data) {
            return null;
        }
        Schema schema = RuntimeSchema.getSchema(Company.class);
        Company company = new Company();
        ProtostuffIOUtil.mergeFrom(data, company, schema);
        return company;
    }

}
