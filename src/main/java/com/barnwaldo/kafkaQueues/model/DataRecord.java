package com.barnwaldo.kafkaQueues.model;

import lombok.*;

@Getter @Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class DataRecord {
    private int id;
    private String key;
    private String name;
    private int dataField1;
    private double dataField2;
}
