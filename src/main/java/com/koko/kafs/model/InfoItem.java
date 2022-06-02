package com.koko.kafs.model;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class InfoItem {
    long id;
    String name;
    Timestamp ts;
    double value;

    public InfoItem(long i, String s, Timestamp valueOf, double i1) {
        this.id = i;
        this.name = s;
        this.ts = valueOf;
        this.value = i1;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder().append("{");
        sb.append("id: ").append(id);
        sb.append(", name: ").append(name);
        sb.append(", ts: ").append(ts);
        sb.append(", value: ").append(value);
        sb.append("}");

        return sb.toString();
    }
}
