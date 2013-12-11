package com.livingsocial.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * IsoWeekAdd.
 *
 */
@Description(name = "iso_week_add",
    value = "_FUNC_(iso_week,weeks) - Adds the given amount of weeks to the given ISO week.",
    extended = "Examples:\n"
    + "  > SELECT _FUNC_('2004-W52',1) FROM src LIMIT 1;\n 2004-W53\n"
    + "  > SELECT _FUNC_('2004-W53',1) FROM src LIMIT 1;\n 2005-W01\n"
    + "  > SELECT _FUNC_('2005-W52',-1) FROM src LIMIT 1;\n 2005-W51\n")
public class IsoWeekAdd extends UDF {
  private final DateTimeFormatter weekDateFormatter = ISODateTimeFormat.weekyearWeek().withZoneUTC();

  /**
   * Get the week of the year from a date string.
   *
   * @param dateString
   *          the dateString in the format of "xxxx-'W'ww".
   *
   * @return an String representing the iso week.
   */
  public Text evaluate(Text dateString, int weeks) {
    if (dateString == null) {
      return null;
    }
    try {
		DateTime date = weekDateFormatter.parseDateTime(dateString.toString());
        DateTime result = date.plusWeeks(weeks);
        String isoWeekDate = weekDateFormatter.print(result);
        return new Text(isoWeekDate);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }


}
