package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * IsoWeekDate.
 *
 */
@Description(name = "iso_date",
    value = "_FUNC_(date) - Returns the year of ISO date of the given date.",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('2005-01-01') FROM src LIMIT 1;\n 2005-01-01\n"
    + "  > SELECT _FUNC_('1980-12-31 12:59:59') FROM src LIMIT 1;\n 1980-12-31\n")
public class IsoDate extends UDF {
  private final DateTimeFormatter dateOptionalTimeFormatter = ISODateTimeFormat.dateOptionalTimeParser().withZoneUTC();
  private final DateTimeFormatter dateFormatter = ISODateTimeFormat.date().withZoneUTC();

  /**
   * Get the iso date from a date string.
   *
   * @param dateString
   *          the dateString in the format of "yyyy-MM-dd HH:mm:ss" or
   *          "yyyy-MM-dd".
   * @return an String representing the iso week date.
   */
  public Text evaluate(Text dateString) {
    if (dateString == null) {
      return null;
    }
    try {
		DateTime date = dateOptionalTimeFormatter.parseDateTime(dateString.toString());
        String isoWeekDate = dateFormatter.print(date);
        return new Text(isoWeekDate);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }


}
