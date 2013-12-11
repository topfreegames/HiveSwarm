package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * IsoWeekDate.
 *
 */
@Description(name = "iso_week_date",
    value = "_FUNC_(date) - Returns the year of ISO week date of the given date.",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('2005-01-01') FROM src LIMIT 1;\n 2004-W53-6\n"
    + "  > SELECT _FUNC_('2005-12-31') FROM src LIMIT 1;\n 2005-W52-6\n")
public class IsoWeekDate extends UDF {
  private final DateTimeFormatter weekDateFormatter = ISODateTimeFormat.weekDate().withZoneUTC();
  private final DateTimeFormatter dateFormatter = ISODateTimeFormat.date().withZoneUTC();

  /**
   * Get the week of the year from a date string.
   *
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd".
   *
   * @return an String representing the iso week date.
   */
  public Text evaluate(Text dateString) {
    if (dateString == null) {
      return null;
    }
    try {
		DateTime date = dateFormatter.parseDateTime(dateString.toString());
        String isoWeekDate = weekDateFormatter.print(date);
        return new Text(isoWeekDate);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }


}
