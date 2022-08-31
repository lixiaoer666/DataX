package com.alibaba.datax.plugin.reader.mongodbreader;


import com.alibaba.datax.common.element.DateColumn;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MongoDBReaderTest {
    public static void main(String[] args) {
        String in = "2022082912";
        SimpleDateFormat timeStrFormatter = new SimpleDateFormat("yyyyMMddHH");
        Date parse = timeStrFormatter.parse(in, new ParsePosition(0));
        System.out.println(parse);
    }
}