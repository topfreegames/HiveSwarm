package com.livingsocial.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by leobessa on 12/11/13.
 */
public class IsoWeekAddTest {
    @Test
    public void testEvaluate() throws Exception {
        IsoWeekAdd subject = new IsoWeekAdd();
        assertEquals(new Text("2004-W53"),subject.evaluate(new Text("2004-W52"),1));
        assertEquals(new Text("2005-W01"),subject.evaluate(new Text("2004-W53"),1));
        assertEquals(new Text("2004-W53"),subject.evaluate(new Text("2005-W01"),-1));
        assertEquals(new Text("2005-W51"),subject.evaluate(new Text("2005-W52"),-1));
    }
}
