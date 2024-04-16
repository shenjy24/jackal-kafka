package com.jonas.kafka.company;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Company
 *
 * @author shenjy
 * @version 1.0
 * @date 2021-08-29
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Company {
    private String name;
    private String address;
}
