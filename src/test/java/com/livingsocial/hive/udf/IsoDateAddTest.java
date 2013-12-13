package com.livingsocial.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by leobessa on 12/13/13.
 */
public class IsoDateAddTest {
    @Test
    public void testEvaluate() throws Exception {
        IsoDateAdd subject = new IsoDateAdd();
        assertEquals(new Text("2004-01-02"), subject.evaluate(new Text("2004-01-01"), 1));
    }
}
