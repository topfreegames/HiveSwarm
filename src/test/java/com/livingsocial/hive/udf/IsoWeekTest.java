package com.livingsocial.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by leobessa on 12/11/13.
 */
public class IsoWeekTest {
    @Test
    public void testEvaluate() throws Exception {
        IsoWeek subject = new IsoWeek();
        assertEquals("2004-W53",subject.evaluate(new Text("2005-01-01")).toString());
        assertEquals("2005-W52",subject.evaluate(new Text("2005-12-31")).toString());
    }
}
