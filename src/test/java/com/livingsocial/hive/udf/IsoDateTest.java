package com.livingsocial.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.text.SimpleDateFormat;

import static org.junit.Assert.*;

/**
 * Created by leobessa on 12/11/13.
 */
public class IsoDateTest {

    @Test
    public void testBasicEvaluate() throws Exception {
        IsoDate subject = new IsoDate();
        Text result = subject.evaluate(new Text("2005-01-01"));
        assertEquals("2005-01-01",result.toString());
    }

    @Test
    public void testEvaluateWithTime() throws Exception {
        IsoDate subject = new IsoDate();
        Text result = subject.evaluate(new Text("1980-12-31T12:59:59"));
        assertEquals("1980-12-31",result.toString());
    }

}
