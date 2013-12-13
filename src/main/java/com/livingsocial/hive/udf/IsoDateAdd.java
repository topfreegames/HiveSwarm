package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * IsoDateAdd.
 *
 */
@Description(name = "iso_date_add",
    value = "_FUNC_(iso_date,days) - Adds the given amount of days to the given ISO date.",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('2013-01-01',1) FROM src LIMIT 1;\n 2004-01-02\n")

public class IsoDateAdd extends UDF {
  private final DateTimeFormatter dateFormatter = ISODateTimeFormat.date().withZoneUTC();

  /**
   * Get the week of the year from a date string.
   *
   * @param dateString
   *          the dateString in the iso date format.
   *
   * @return an String representing the iso date.
   */
  public Text evaluate(Text dateString, int days) {
    if (dateString == null) {
      return null;
    }
    try {
		DateTime date = dateFormatter.parseDateTime(dateString.toString());
        DateTime result = date.plusDays(days);
        String isoDateResult = dateFormatter.print(result);
        return new Text(isoDateResult);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }


}
