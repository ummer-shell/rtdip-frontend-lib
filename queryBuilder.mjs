import nunjucks from "nunjucks";
import moment from "moment";

nunjucks.configure({ autoescape: true });
var env = new nunjucks.Environment();
// Custom filter to join array items with quotes
env.addFilter('joinWithQuotes', function(arr, delimiter = ', ') {
    const result = arr.map(item => `"${item}"`).join(delimiter);
    return new nunjucks.runtime.SafeString(result);
});

const TIMESTAMP_FORMAT = "YYYY-MM-DDTHH:mm:ssZ"
const seconds_per_unit = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


export function timeWeightedAverage(parameters) {
    parameters = _parse_dates(parameters)
    parameters["range_join_seconds"] = _convert_to_seconds(
        parameters["time_interval_rate"]
        + " "
        + parameters["time_interval_unit"][0]
    )

    parameters["start_datetime"] = 
    moment(parameters.start_date, TIMESTAMP_FORMAT)
    .format("YYYY-MM-DDTHH:mm:ss");

    parameters["end_datetime"] = 
    moment(parameters.end_date, TIMESTAMP_FORMAT)
    .format("YYYY-MM-DDTHH:mm:ss");

    let timeWeightedAverageQuery = `
        WITH raw_events AS (SELECT DISTINCT \`{{ tagname_column }}\`, from_utc_timestamp(to_timestamp(date_format(\`{{ timestamp_column }}\`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"{{ time_zone }}\") AS \`{{ timestamp_column }}\`, {%if include_status and include_status == True %} \`{{ status_column }}\`, {% else %} 'Good' AS \`Status\`, {% endif %} \`{{ value_column }}\` FROM
        {% if source is defined and source is not none %}
        {{ source|lower }} 
        {% else %}
        \`{{ business_unit|lower }}\`.\`sensors\`.\`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}\` 
        {% endif %}
        WHERE to_date(\`{{ timestamp_column }}\`) BETWEEN date_sub(to_date(to_timestamp(\"{{ start_date }}\")), {{ window_length }}) AND date_add(to_date(to_timestamp(\"{{ end_date }}\")), {{ window_length }}) AND\`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }})  
        {% if include_status is defined and include_status == True and include_bad_data is defined and include_bad_data == False %} AND \`{{ status_column }}\` = 'Good' {% endif %}) 
        ,date_array AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp(\"{{ start_date }}\"), \"{{ time_zone }}\"), from_utc_timestamp(to_timestamp(\"{{ end_date }}\"), \"{{ time_zone }}\"), INTERVAL '{{ time_interval_rate + ' ' + time_interval_unit }}')) AS \`{{ timestamp_column }}\`, explode(array({{ tag_names | joinWithQuotes }})) AS \`{{ tagname_column }}\`) 
        ,boundary_events AS (SELECT coalesce(a.\`{{ tagname_column }}\`, b.\`{{ tagname_column }}\`) AS \`{{ tagname_column }}\`, coalesce(a.\`{{ timestamp_column }}\`, b.\`{{ timestamp_column }}\`) AS \`{{ timestamp_column }}\`, b.\`{{ status_column }}\`, b.\`{{ value_column }}\` FROM date_array a FULL OUTER JOIN raw_events b ON a.\`{{ timestamp_column }}\` = b.\`{{ timestamp_column }}\` AND a.\`{{ tagname_column }}\` = b.\`{{ tagname_column }}\`) 
        ,window_buckets AS (SELECT \`{{ timestamp_column }}\` AS window_start, LEAD(\`{{ timestamp_column }}\`) OVER (ORDER BY \`{{ timestamp_column }}\`) AS window_end FROM (SELECT distinct\`{{ timestamp_column }}\` FROM date_array) ) 
        ,window_events AS (SELECT /*+ RANGE_JOIN(b, {{ range_join_seconds }} ) */ b.\`{{ tagname_column }}\`, b.\`{{ timestamp_column }}\`, a.window_start AS \`WindowEventTime\`, b.\`{{ status_column }}\`, b.\`{{ value_column }}\` FROM boundary_events b LEFT OUTER JOIN window_buckets a ON a.window_start <= b.\`{{ timestamp_column }}\` AND a.window_end > b.\`{{ timestamp_column }}\`) 
        ,fill_status AS (SELECT *, last_value(\`{{ status_column }}\`, true) OVER (PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \`Fill_{{ status_column }}\`, CASE WHEN \`Fill_{{ status_column }}\` = "Good" THEN \`{{ value_column }}\` ELSE null END AS \`Good_{{ value_column }}\` FROM window_events) 
        ,fill_value AS (SELECT *, last_value(\`Good_{{ value_column }}\`, true) OVER (PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \`Fill_{{ value_column }}\` FROM fill_status) 
        {% if step is defined and step == "metadata" %} 
        ,fill_step AS (SELECT *, IFNULL(Step, false) AS Step FROM fill_value f LEFT JOIN 
        {% if source_metadata is defined and source_metadata is not none %}
        \`{{ source_metadata|lower }}\` 
        {% else %}
        \`{{ business_unit|lower }}\`.\`sensors\`.\`{{ asset|lower }}_{{ data_security_level|lower }}_metadata\` 
        {% endif %}
        m ON f.\`{{ tagname_column }}\` = m.\`{{ tagname_column }}\`) 
        {% else %}
        ,fill_step AS (SELECT *, {{ step }} AS Step FROM fill_value) 
        {% endif %}
        ,interpolate AS (SELECT *, CASE WHEN \`Step\` = false AND \`{{ status_column }}\` IS NULL AND \`{{ value_column }}\` IS NULL THEN lag(\`{{ timestamp_column }}\`) OVER ( PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\` ) ELSE NULL END AS \`Previous_{{ timestamp_column }}\`, CASE WHEN \`Step\` = false AND \`{{ status_column }}\` IS NULL AND \`{{ value_column }}\` IS NULL THEN lag(\`Fill_{{ value_column }}\`) OVER ( PARTITION BY \`{{ tagname_column }}\` ORDER BY\`{{ timestamp_column }}\` ) ELSE NULL END AS \`Previous_Fill_{{ value_column }}\`, 
        lead(\`{{ timestamp_column }}\`) OVER ( PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\` ) AS \`Next_{{ timestamp_column }}\`, CASE WHEN \`Step\` = false AND\`Status\` IS NULL AND \`{{ value_column }}\` IS NULL THEN lead(\`Fill_{{ value_column }}\`) OVER ( PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\` ) ELSE NULL END AS \`Next_Fill_{{ value_column }}\`, CASE WHEN \`Step\` = false AND \`{{ status_column }}\` IS NULL AND \`{{ value_column }}\` IS NULL THEN \`Previous_Fill_{{ value_column }}\` + ( (\`Next_Fill_{{ value_column }}\` - \`Previous_Fill_{{ value_column }}\`) * ( ( unix_timestamp(\`{{ timestamp_column }}\`) - unix_timestamp(\`Previous_{{ timestamp_column }}\`) ) / ( unix_timestamp(\`Next_{{ timestamp_column }}\`) - unix_timestamp(\`Previous_{{ timestamp_column }}\`) ) ) ) ELSE NULL END AS \`Interpolated_{{ value_column }}\`, coalesce(\`Interpolated_{{ value_column }}\`, \`Fill_{{ value_column }}\`) as \`Event_{{ value_column }}\` FROM fill_step )
        ,twa_calculations AS (SELECT \`{{ tagname_column }}\`, \`{{ timestamp_column }}\`, \`Window{{ timestamp_column }}\`, \`Step\`, \`{{ status_column }}\`,\`{{ value_column }}\`, \`Previous_{{ timestamp_column }}\`, \`Previous_Fill_{{ value_column }}\`, \`Next_{{ timestamp_column }}\`, \`Next_Fill_{{ value_column }}\`, \`Interpolated_{{ value_column }}\`, \`Fill_{{ status_column }}\`, \`Fill_{{ value_column }}\`, \`Event_{{ value_column }}\`, lead(\`Fill_{{ status_column }}\`) OVER (PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\`) AS \`Next_{{ status_column }}\` 
        , CASE WHEN \`Next_{{ status_column }}\` = "Good" OR (\`Fill_{{ status_column }}\` = "Good" AND \`Next_{{ status_column }}\` != "Good") THEN lead(\`Event_{{ value_column }}\`) OVER (PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\`) ELSE \`{{ value_column }}\` END AS \`Next_{{ value_column }}_For_{{ status_column }}\` 
        , CASE WHEN \`Fill_{{ status_column }}\` = "Good" THEN \`Next_{{ value_column }}_For_{{ status_column }}\` ELSE 0 END AS \`Next_{{ value_column }}\` 
        , CASE WHEN \`Fill_{{ status_column }}\` = "Good" AND \`Next_{{ status_column }}\` = "Good" THEN ((cast(\`Next_{{ timestamp_column }}\` AS double) - cast(\`{{ timestamp_column }}\` AS double)) / 60) WHEN \`Fill_{{ status_column }}\` = "Good" AND \`Next_{{ status_column }}\` != "Good" THEN ((cast(\`Next_{{ timestamp_column }}\` AS integer) - cast(\`{{ timestamp_column }}\` AS double)) / 60) ELSE 0 END AS good_minutes 
        , CASE WHEN Step == false THEN ((\`Event_{{ value_column }}\` + \`Next_{{ value_column }}\`) * 0.5) * good_minutes ELSE (\`Event_{{ value_column }}\` * good_minutes) END AS twa_value FROM interpolate) 
        ,twa AS (SELECT \`{{ tagname_column }}\`, \`Window{{ timestamp_column }}\` AS \`{{ timestamp_column }}\`, sum(twa_value) / sum(good_minutes) AS \`{{ value_column }}\` from twa_calculations GROUP BY \`{{ tagname_column }}\`, \`Window{{ timestamp_column }}\`) 
        ,project AS (SELECT * FROM twa WHERE \`{{ timestamp_column }}\` BETWEEN to_timestamp("{{ start_datetime }}") AND to_timestamp("{{ end_datetime }}")) 
        {% if pivot is defined and pivot == true %}
        ,pivot AS (SELECT * FROM project PIVOT (FIRST(\`{{ value_column }}\`) FOR \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}))) 
        SELECT * FROM pivot ORDER BY \`{{ timestamp_column }}\` 
        {% else %}
        SELECT * FROM project ORDER BY \`{{ tagname_column }}\`, \`{{ timestamp_column }}\` 
        {% endif %}
        {% if limit is defined and limit is not none %}
        LIMIT {{ limit }} 
        {% endif %}
        {% if offset is defined and offset is not none %}
        OFFSET {{ offset }} 
        {% endif %}
    `;

    const timeWeightedAverageParameters = {
        "source":               parameters["source"],
        "source_metadata":      parameters["source_metadata"],
        "business_unit":        parameters["business_unit"],
        "region":               parameters["region"],
        "asset":                parameters["asset"],
        "data_security_level":  parameters["data_security_level"],
        "data_type":            parameters["data_type"],
        "start_date":           parameters["start_date"],
        "end_date":             parameters["end_date"],
        "start_datetime":       parameters["start_datetime"],
        "end_datetime":         parameters["end_datetime"],
        "tag_names":            [...new Set(parameters["tag_names"])],
        "time_interval_rate":   parameters["time_interval_rate"],
        "time_interval_unit":   parameters["time_interval_unit"],
        "window_length":        parameters["window_length"],
        "include_bad_data":     parameters["include_bad_data"],
        "step":                 parameters["step"],
        "pivot":                parameters["pivot"],
        "limit":                parameters["limit"],
        "offset":               parameters["offset"],
        "time_zone":            parameters["time_zone"],
        "range_join_seconds":   parameters["range_join_seconds"],
        "tagname_column":       get(parameters, "tagname_column", "TagName"),
        "timestamp_column":     get(parameters, "timestamp_column", "EventTime"),
        "include_status":       "status_column" in parameters && parameters["status_column"] ? false : true,
        "status_column":        "status_column" in parameters && parameters["status_column"] ? "Status" : get(parameters, "status_column", "Status"),
        "value_column":         get(parameters, "value_column", "Value")
    }

    return env.renderString(timeWeightedAverageQuery, timeWeightedAverageParameters)
}


export function raw(parameters){

    parameters = _parse_dates(parameters)

    let rawQuery = `
        SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(\`{{ timestamp_column }}\`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"{{ time_zone }}\") AS \`{{ timestamp_column }}\`, \`{{ tagname_column }}\`, {% if include_status is defined and include_status == true %} \`{{ status_column }}\`, {% endif %} \`{{ value_column }}\` FROM 
        {% if source is defined and source is not none %}
        {{ source|lower }} 
        {% else %}
        \`{{ business_unit|lower }}\`.\`sensors\`.\`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}\` 
        {% endif %}
        WHERE \`{{ timestamp_column }}\` BETWEEN to_timestamp(\"{{ start_date }}\") AND to_timestamp(\"{{ end_date }}\") AND \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}) 
        {% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %}
        AND \`{{ status_column }}\` = 'Good'
        {% endif %}
        ORDER BY \`{{ tagname_column }}\`, \`{{ timestamp_column }}\` 
        {% if limit is defined and limit is not none %}
        LIMIT {{ limit }} 
        {% endif %}
        {% if offset is defined and offset is not none %}
        OFFSET {{ offset }} 
        {% endif %}
    `

    const rawParameters = {
        "source":               parameters["source"],
        "source_metadata":      parameters["source_metadata"],
        "business_unit":        parameters["business_unit"],
        "region":               parameters["region"],
        "asset":                parameters["asset"],
        "data_security_level":  parameters["data_security_level"],
        "data_type":            parameters["data_type"],
        "start_date":           parameters["start_date"],
        "end_date":             parameters["end_date"],
        "tag_names":            [...new Set(parameters["tag_names"])],
        "time_zone":            parameters["time_zone"],
        "include_bad_data":     parameters["include_bad_data"],
        "limit":                parameters["limit"],
        "offset":               parameters["offset"],
        "tagname_column":       get(parameters, "tagname_column", "TagName"),
        "timestamp_column":     get(parameters, "timestamp_column", "EventTime"),
        "include_status":       "status_column" in parameters && parameters["status_column"] ? false : true,
        "status_column":        "status_column" in parameters && parameters["status_column"] ? "Status" : get(parameters, "status_column", "Status"),
        "value_column":         get(parameters, "value_column", "Value")
    }

    console.log(rawParameters)
    return env.renderString(rawQuery, rawParameters)
}


export function latest(parameters){

    parameters = _parse_dates(parameters)

    let latestQuery = `
        SELECT * FROM 
        {% if source is defined and source is not none %}
        {{ source|lower }}
        {% else %}
        \`{{ business_unit|lower }}\`.\`sensors\`.\`{{ asset|lower }}_{{ data_security_level|lower }}_events_latest\` 
        {% endif %}
        {% if tag_names is defined and tag_names|length > 0 %} 
        WHERE \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}) 
        {% endif %}
        ORDER BY \`{{ tagname_column }}\` 
        {% if limit is defined and limit is not none %}
        LIMIT {{ limit }} 
        {% endif %}
        {% if offset is defined and offset is not none %}
        OFFSET {{ offset }} 
        {% endif %}
    `

    const latestParameters = {
        "source":               parameters["source"],
        "source_metadata":      parameters["source_metadata"],
        "business_unit":        parameters["business_unit"],
        "region":               parameters["region"],
        "asset":                parameters["asset"],
        "data_security_level":  parameters["data_security_level"],
        "tag_names":            [...new Set(parameters["tag_names"])],
        "limit":                get(parameters, "limit", 1),
        "offset":               get(parameters, "offset", 0),
        "tagname_column":       get(parameters, "tagname_column", "TagName")
    }

    return env.renderString(latestQuery, latestParameters)
}



export function resample(parameters){

    parameters = _parse_dates(parameters)
    parameters["range_join_seconds"] = _convert_to_seconds(
        parameters["time_interval_rate"]
        + " "
        + parameters["time_interval_unit"][0]
    )

    let resampleQuery = `
        WITH raw_events AS (SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(\`{{ timestamp_column }}\`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"{{ time_zone }}\") AS \`{{ timestamp_column }}\`, \`{{ tagname_column }}\`, {% if include_status is defined and include_status == true %} \`{{ status_column }}\`, {% else %} 'Good' AS \`Status\`, {% endif %} \`{{ value_column }}\` FROM 
        {% if source is defined and source is not none %}
        {{ source|lower }}
        {% else %}
        \`{{ business_unit|lower }}\`.\`sensors\`.\`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}\` 
        {% endif %}
        WHERE \`{{ timestamp_column }}\` BETWEEN to_timestamp(\"{{ start_date }}\") AND to_timestamp(\"{{ end_date }}\") AND \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}) 
        {% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %} AND \`{{ status_column }}\` = 'Good' {% endif %}) 
        ,date_array AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp("{{ start_date }}"), "{{ time_zone }}"), from_utc_timestamp(to_timestamp("{{ end_date }}"), "{{ time_zone }}"), INTERVAL \'{{ time_interval_rate + \' \' + time_interval_unit }}\')) AS timestamp_array) 
        ,window_buckets AS (SELECT timestamp_array AS window_start, LEAD(timestamp_array) OVER (ORDER BY timestamp_array) AS window_end FROM date_array) 
        ,resample AS (SELECT /*+ RANGE_JOIN(d, {{ range_join_seconds }} ) */ d.window_start, d.window_end, e.\`{{ tagname_column }}\`, {{ agg_method }}(e.\`{{ value_column }}\`) OVER (PARTITION BY e.\`{{ tagname_column }}\`, d.window_start ORDER BY e.\`{{ timestamp_column }}\` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \`{{ value_column }}\` FROM window_buckets d INNER JOIN raw_events e ON d.window_start <= e.\`{{ timestamp_column }}\` AND d.window_end > e.\`{{ timestamp_column }}\`) 
        ,project AS (SELECT window_start AS \`{{ timestamp_column }}\`, \`{{ tagname_column }}\`, \`{{ value_column }}\` FROM resample GROUP BY window_start, \`{{ tagname_column }}\`, \`{{ value_column }}\` 
        {% if is_resample is defined and is_resample == true %}
        ORDER BY \`{{ tagname_column }}\`, \`{{ timestamp_column }}\` 
        {% endif %}
        ) 
        {% if is_resample is defined and is_resample == true and pivot is defined and pivot == true %}
        ,pivot AS (SELECT * FROM project PIVOT (FIRST(\`{{ value_column }}\`) FOR \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}))) 
        SELECT * FROM pivot ORDER BY \`{{ timestamp_column }}\` 
        {% else %}
        SELECT * FROM project
        {% endif %}
        {% if is_resample is defined and is_resample == true and limit is defined and limit is not none %}
        LIMIT {{ limit }}
        {% endif %}
        {% if is_resample is defined and is_resample == true and offset is defined and offset is not none %}
        OFFSET {{ offset }}
        {% endif %}
    `

    const resampleParameters = {
        "source":               parameters["source"],
        "source_metadata":      parameters["source_metadata"],
        "business_unit":        parameters["business_unit"],
        "region":               parameters["region"],
        "asset":                parameters["asset"],
        "data_security_level":  parameters["data_security_level"],
        "data_type":            parameters["data_type"],
        "start_date":           parameters["start_date"],
        "end_date":             parameters["end_date"],
        "tag_names":            [...new Set(parameters["tag_names"])],
        "include_bad_data":     parameters["include_bad_data"],
        "time_interval_rate":   parameters["time_interval_rate"],
        "time_interval_unit":   parameters["time_interval_unit"],
        "agg_method":           parameters["agg_method"],
        "time_zone":            parameters["time_zone"],
        "pivot":                parameters["pivot"],
        "limit":                parameters["limit"],
        "offset":               parameters["offset"],
        "is_resample":          true,
        "tagname_column":       get(parameters, "tagname_column", "TagName"),
        "timestamp_column":     get(parameters, "timestamp_column", "EventTime"),
        "include_status":       "status_column" in parameters && parameters["status_column"] ? false : true,
        "status_column":        "status_column" in parameters && parameters["status_column"] ? "Status" : get(parameters, "status_column", "Status"),
        "value_column":         get(parameters, "value_column", "Value"),
        "range_join_seconds":   parameters["range_join_seconds"]
    }

    return env.renderString(resampleQuery, resampleParameters)
}



export function interpolate(parameters){

    parameters = _parse_dates(parameters)
    parameters["range_join_seconds"] = _convert_to_seconds(
        parameters["time_interval_rate"]
        + " "
        + parameters["time_interval_unit"][0]
    )

    const interpolateParameters = {
        "source":               parameters["source"],
        "source_metadata":      parameters["source_metadata"],
        "business_unit":        parameters["business_unit"],
        "region":               parameters["region"],
        "asset":                parameters["asset"],
        "data_security_level":  parameters["data_security_level"],
        "data_type":            parameters["data_type"],
        "start_date":           parameters["start_date"],
        "end_date":             parameters["end_date"],
        "tag_names":            [...new Set(parameters["tag_names"])],
        "include_bad_data":     parameters["include_bad_data"],
        "time_interval_rate":   parameters["time_interval_rate"],
        "time_interval_unit":   parameters["time_interval_unit"],
        "agg_method":           get(parameters, "agg_method", "first"),
        "interpolation_method": parameters["interpolation_method"], 
        "time_zone":            parameters["time_zone"],
        "pivot":                parameters["pivot"],
        "limit":                parameters["limit"],
        "offset":               parameters["offset"],
        "is_resample":          true,
        "tagname_column":       get(parameters, "tagname_column", "TagName"),
        "timestamp_column":     get(parameters, "timestamp_column", "EventTime"),
        "include_status":       "status_column" in parameters && parameters["status_column"] ? false : true,
        "status_column":        "status_column" in parameters && parameters["status_column"] ? "Status" : get(parameters, "status_column", "Status"),
        "value_column":         get(parameters, "value_column", "Value"),
        "range_join_seconds":   parameters["range_join_seconds"]
    }

    if(parameters["interpolation_method"] == "forward_fill"){
        var interpolationMethods = "last_value/UNBOUNDED PRECEDING/CURRENT ROW"
    }
    
    if(parameters["interpolation_method"] == "backward_fill"){
        var interpolationMethods = "first_value/CURRENT ROW/UNBOUNDED FOLLOWING"
    }
    
    if (parameters["interpolation_method"] == "forward_fill" || parameters["interpolation_method"] == "backward_fill"){
        var interpolationOptions = interpolationMethods.split("/")
    }

    interpolateParameters["interpolation_method"] = parameters["interpolation_method"]
    if (parameters["interpolation_method"] == "forward_fill" || parameters["interpolation_method"] == "backward_fill"){
        interpolateParameters["interpolation_options_0"] = interpolationOptions[0]
        interpolateParameters["interpolation_options_1"] = interpolationOptions[1]
        interpolateParameters["interpolation_options_2"] = interpolationOptions[2]
    }

    // Generate resample query first as the basis of interpolate query
    interpolateParameters["pivot"] = false // make sure to not pivot with initial query
    const resampleQuery = resample(interpolateParameters)

    // reset parameters after initial resample starter query 
    interpolateParameters["is_resample"] = false
    interpolateParameters["pivot"] = parameters["pivot"]

    // Additional part of the interpolate query, which builds on the resample
    let interpolateQuery = `
        WITH resample AS (${resampleQuery})
        ,date_array AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp(\"{{ start_date }}\"), \"{{ time_zone }}\"), from_utc_timestamp(to_timestamp(\"{{ end_date }}\"), \"{{ time_zone }}\"), INTERVAL '{{ time_interval_rate + ' ' + time_interval_unit }}')) AS \`{{ timestamp_column }}\`, explode(array({{ tag_names | joinWithQuotes }})) AS \`{{ tagname_column }}\`) 
        {% if (interpolation_method is defined) and (interpolation_method == "forward_fill" or interpolation_method == "backward_fill") %}
        ,project AS (SELECT a.\`{{ timestamp_column }}\`, a.\`{{ tagname_column }}\`, {{ interpolation_options_0 }}(b.\`{{ value_column }}\`, true) OVER (PARTITION BY a.\`{{ tagname_column }}\` ORDER BY a.\`{{ timestamp_column }}\` ROWS BETWEEN {{ interpolation_options_1 }} AND {{ interpolation_options_2 }}) AS \`{{ value_column }}\` FROM date_array a LEFT OUTER JOIN resample b ON a.\`{{ timestamp_column }}\` = b.\`{{ timestamp_column }}\` AND a.\`{{ tagname_column }}\` = b.\`{{ tagname_column }}\`) 
        {% elif (interpolation_method is defined) and (interpolation_method == "linear") %}
        ,linear_interpolation_calculations AS (SELECT coalesce(a.\`{{ tagname_column }}\`, b.\`{{ tagname_column }}\`) AS \`{{ tagname_column }}\`, coalesce(a.\`{{ timestamp_column }}\`, b.\`{{ timestamp_column }}\`) AS \`{{ timestamp_column }}\`, a.\`{{ timestamp_column }}\` AS \`Requested_{{ timestamp_column }}\`, b.\`{{ timestamp_column }}\` AS \`Found_{{ timestamp_column }}\`, b.\`{{ value_column }}\`, 
        last_value(b.\`{{ timestamp_column }}\`, true) OVER (PARTITION BY a.\`{{ tagname_column }}\` ORDER BY a.\`{{ timestamp_column }}\` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \`Last_{{ timestamp_column }}\`, last_value(b.\`{{ value_column }}\`, true) OVER (PARTITION BY a.\`{{ tagname_column }}\` ORDER BY a.\`{{ timestamp_column }}\` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS \`Last_{{ value_column }}\`, 
        first_value(b.\`{{ timestamp_column }}\`, true) OVER (PARTITION BY a.\`{{ tagname_column }}\` ORDER BY a.\`{{ timestamp_column }}\` ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS \`Next_{{ timestamp_column }}\`, first_value(b.\`{{ value_column }}\`, true) OVER (PARTITION BY a.\`{{ tagname_column }}\` ORDER BY a.\`{{ timestamp_column }}\` ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS \`Next_{{ value_column }}\`, 
        CASE WHEN b.\`{{ value_column }}\` is NULL THEN \`Last_{{ value_column }}\` + (unix_timestamp(a.\`{{ timestamp_column }}\`) - unix_timestamp(\`Last_{{ timestamp_column }}\`)) * ((\`Next_{{ value_column }}\` - \`Last_{{ value_column }}\`)) / ((unix_timestamp(\`Next_{{ timestamp_column }}\`) - unix_timestamp(\`Last_{{ timestamp_column }}\`))) ELSE b.\`{{ value_column }}\` END AS \`linear_interpolated_{{ value_column }}\` FROM date_array a FULL OUTER JOIN resample b ON a.\`{{ timestamp_column }}\` = b.\`{{ timestamp_column }}\` AND a.\`{{ tagname_column }}\` = b.\`{{ tagname_column }}\`) 
        ,project AS (SELECT \`{{ timestamp_column }}\`, \`{{ tagname_column }}\`, \`linear_interpolated_{{ value_column }}\` AS \`{{ value_column }}\` FROM linear_interpolation_calculations) 
        {% else %}
        ,project AS (SELECT * FROM resample) 
        {% endif %}
        {% if pivot is defined and pivot == true %}
        ,pivot AS (SELECT * FROM project PIVOT (FIRST(\`{{ value_column }}\`) FOR \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}))) 
        SELECT * FROM pivot ORDER BY \`{{ timestamp_column }}\` 
        {% else %}
        SELECT * FROM project ORDER BY \`{{ tagname_column }}\`, \`{{ timestamp_column }}\` 
        {% endif %}
        {% if limit is defined and limit is not none %}
        LIMIT {{ limit }} 
        {% endif %}
        {% if offset is defined and offset is not none %}
        OFFSET {{ offset }} 
        {% endif %}
    `

    return env.renderString(interpolateQuery, interpolateParameters)
}



export function interpolationAtTime(parameters){

    // parse times and remove duplicates
    parameters = _parse_dates(parameters)
    parameters["timestamps"] = [...new Set(parameters["timestamps"])]
    console.log(parameters["timestamps"])
    parameters["min_timestamp"] = new Date(Math.min(...parameters["timestamps"].map(dateStr => new Date(dateStr)))).toISOString()
    parameters["max_timestamp"] = new Date(Math.max(...parameters["timestamps"].map(dateStr => new Date(dateStr)))).toISOString() 

    let interpolationAtTimeQuery = `
        WITH raw_events AS (SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(\`{{ timestamp_column }}\`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"{{ time_zone }}\") AS \`{{ timestamp_column }}\`, \`{{ tagname_column }}\`, {% if include_status is defined and include_status == true %} \`{{ status_column }}\`, {% else %} 'Good' AS \`Status\`, {% endif %} \`{{ value_column }}\` FROM 
        {% if source is defined and source is not none %}
        {{ source|lower }} 
        {% else %}
        \`{{ business_unit|lower }}\`.\`sensors\`.\`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}\` 
        {% endif %}
        WHERE to_date(\`{{ timestamp_column }}\`) BETWEEN 
        {% if timestamps is defined %} 
        date_sub(to_date(to_timestamp("{{ min_timestamp }}")), {{ window_length }}) AND date_add(to_date(to_timestamp("{{ max_timestamp }}")), {{ window_length}})
        {% endif %} AND \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}) 
        {% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %} AND \`{{ status_column }}\` = 'Good' {% endif %}) 
        , date_array AS (SELECT explode(array( 
        {% for timestamp in timestamps -%} 
        from_utc_timestamp(to_timestamp("{{timestamp}}"), "{{time_zone}}") 
        {% if not loop.last %} , {% endif %} {% endfor %} )) AS \`{{ timestamp_column }}\`, 
        explode(array({{ tag_names | joinWithQuotes }})) AS \`{{ tagname_column }}\`) 
        , interpolation_events AS (SELECT coalesce(a.\`{{ tagname_column }}\`, b.\`{{ tagname_column }}\`) AS \`{{ tagname_column }}\`, coalesce(a.\`{{ timestamp_column }}\`, b.\`{{ timestamp_column }}\`) AS \`{{ timestamp_column }}\`, a.\`{{ timestamp_column }}\` AS \`Requested_{{ timestamp_column }}\`, b.\`{{ timestamp_column }}\` AS \`Found_{{ timestamp_column }}\`, b.\`{{ status_column }}\`, b.\`{{ value_column }}\` FROM date_array a FULL OUTER JOIN  raw_events b ON a.\`{{ timestamp_column }}\` = b.\`{{ timestamp_column }}\` AND a.\`{{ tagname_column }}\` = b.\`{{ tagname_column }}\`) 
        , interpolation_calculations AS (SELECT *, lag(\`{{ timestamp_column }}\`) OVER (PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\`) AS \`Previous_{{ timestamp_column }}\`, lag(\`{{ value_column }}\`) OVER (PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\`) AS \`Previous_{{ value_column }}\`, lead(\`{{ timestamp_column }}\`) OVER (PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\`) AS \`Next_{{ timestamp_column }}\`, lead(\`{{ value_column }}\`) OVER (PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\`) AS \`Next_{{ value_column }}\`, 
        CASE WHEN \`Requested_{{ timestamp_column }}\` = \`Found_{{ timestamp_column }}\` THEN \`{{ value_column }}\` WHEN \`Next_{{ timestamp_column }}\` IS NULL THEN \`Previous_{{ value_column }}\` WHEN \`Previous_{{ timestamp_column }}\` IS NULL AND \`Next_{{ timestamp_column }}\` IS NULL THEN NULL 
        ELSE \`Previous_{{ value_column }}\` + ((\`Next_{{ value_column }}\` - \`Previous_{{ value_column }}\`) * ((unix_timestamp(\`{{ timestamp_column }}\`) - unix_timestamp(\`Previous_{{ timestamp_column }}\`)) / (unix_timestamp(\`Next_{{ timestamp_column }}\`) - unix_timestamp(\`Previous_{{ timestamp_column }}\`)))) END AS \`Interpolated_{{ value_column }}\` FROM interpolation_events) 
        ,project AS (SELECT \`{{ tagname_column }}\`, \`{{ timestamp_column }}\`, \`Interpolated_{{ value_column }}\` AS \`{{ value_column }}\` FROM interpolation_calculations WHERE \`{{ timestamp_column }}\` IN ( 
        {% for timestamp in timestamps -%} 
        from_utc_timestamp(to_timestamp("{{timestamp}}"), "{{time_zone}}") 
        {% if not loop.last %} , {% endif %} {% endfor %}) 
        ) 
        {% if pivot is defined and pivot == true %}
        ,pivot AS (SELECT * FROM project PIVOT (FIRST(\`{{ value_column }}\`) FOR \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}))) 
        SELECT * FROM pivot ORDER BY \`{{ timestamp_column }}\` 
        {% else %}
        SELECT * FROM project ORDER BY \`{{ tagname_column }}\`, \`{{ timestamp_column }}\` 
        {% endif %}
        {% if limit is defined and limit is not none %}
        LIMIT {{ limit }} 
        {% endif %}
        {% if offset is defined and offset is not none %}
        OFFSET {{ offset }} 
        {% endif %}
    `

    const interpolationAtTimeParameters = {
        "source":               parameters["source"],
        "source_metadata":      parameters["source_metadata"],
        "business_unit":        parameters["business_unit"],
        "region":               parameters["region"],
        "asset":                parameters["asset"],
        "data_security_level":  parameters["data_security_level"],
        "data_type":            parameters["data_type"],
        "timestamps":           parameters["timestamps"],
        "time_zone":            parameters["time_zone"],
        "tag_names":            [...new Set(parameters["tag_names"])],
        "include_bad_data":     parameters["include_bad_data"],
        "time_zone":            parameters["time_zone"],
        "min_timestamp":        parameters["min_timestamp"],
        "max_timestamp":        parameters["max_timestamp"],
        "window_length":        parameters["window_length"],
        "pivot":                parameters["pivot"],
        "limit":                parameters["limit"],
        "offset":               parameters["offset"],
        "tagname_column":       get(parameters, "tagname_column", "TagName"),
        "timestamp_column":     get(parameters, "timestamp_column", "EventTime"),
        "include_status":       "status_column" in parameters && parameters["status_column"] ? false : true,
        "status_column":        "status_column" in parameters && parameters["status_column"] ? "Status" : get(parameters, "status_column", "Status"),
        "value_column":         get(parameters, "value_column", "Value"),
    }

    console.log(interpolationAtTimeParameters)
    return env.renderString(interpolationAtTimeQuery, interpolationAtTimeParameters)
}



export function metadata(parameters){

    let metadataQuery = `
        SELECT * FROM 
        {% if source is defined and source is not none %}
        {{ source|lower }}
        {% else %}
        \`{{ business_unit|lower }}\`.\`sensors\`.\`{{ asset|lower }}_{{ data_security_level|lower }}_metadata\` 
        {% endif %}
        {% if tag_names is defined and tag_names|length > 0 %} 
        WHERE \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}) 
        {% endif %}
        ORDER BY \`{{ tagname_column }}\` 
        {% if limit is defined and limit is not none %}
        LIMIT {{ limit }} 
        {% endif %}
        {% if offset is defined and offset is not none %}
        OFFSET {{ offset }} 
        {% endif %}
    `

    const metadataParameters = {
        "source":               parameters["source"],
        "business_unit":        parameters["business_unit"],
        "region":               parameters["region"],
        "asset":                parameters["asset"],
        "data_security_level":  parameters["data_security_level"],
        "data_type":            parameters["data_type"],
        "tag_names":            [...new Set(parameters["tag_names"])],
        "include_bad_data":     parameters["include_bad_data"],
        "limit":                parameters["limit"],
        "offset":               parameters["offset"],
        "tagname_column":       get(parameters, "tagname_column", "TagName")
    }

    return env.renderString(metadataQuery, metadataParameters)
}

export function circularAverage(parameters){
    parameters["circular_function"] = "average"
    return circularStatsQuery(parameters)
}

export function circularStdev(parameters){
    parameters["circular_function"] = "standard_deviation"
    return circularStatsQuery(parameters)
}


function circularStatsQuery(parameters){

    // parse datees
    parameters = _parse_dates(parameters)

    const circularBaseQuery = `
        WITH raw_events AS (SELECT \`{{ timestamp_column }}\`, \`{{ tagname_column }}\`, {% if include_status is defined and include_status == true %} \`{{ status_column }}\`, {% else %} 'Good' AS \`Status\`, {% endif %} \`{{ value_column }}\` FROM 
        {% if source is defined and source is not none %}
        {{ source|lower }} 
        {% else %}
        \`{{ business_unit|lower }}\`.\`sensors\`.\`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}\` 
        {% endif %}
        WHERE \`{{ timestamp_column }}\` BETWEEN TO_TIMESTAMP(\"{{ start_date }}\") AND TO_TIMESTAMP(\"{{ end_date }}\") AND \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}) 
        {% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %} AND \`{{ status_column }}\` = 'Good' {% endif %}) 
        ,date_array AS (SELECT EXPLODE(SEQUENCE(FROM_UTC_TIMESTAMP(TO_TIMESTAMP(\"{{ start_date }}\"), \"{{ time_zone }}\"), FROM_UTC_TIMESTAMP(TO_TIMESTAMP(\"{{ end_date }}\"), \"{{ time_zone }}\"), INTERVAL '{{ time_interval_rate + ' ' + time_interval_unit }}')) AS \`{{ timestamp_column }}\`, EXPLODE(ARRAY('{{ tag_names | joinWithQuotes }}')) AS \`{{ tagname_column }}\`)  
        ,window_events AS (SELECT COALESCE(a.\`{{ tagname_column }}\`, b.\`{{ tagname_column }}\`) AS \`{{ tagname_column }}\`, COALESCE(a.\`{{ timestamp_column }}\`, b.\`{{ timestamp_column }}\`) AS \`{{ timestamp_column }}\`, WINDOW(COALESCE(a.\`{{ timestamp_column }}\`, b.\`{{ timestamp_column }}\`), '{{ time_interval_rate + ' ' + time_interval_unit }}').START \`Window{{ timestamp_column }}\`, b.\`{{ status_column }}\`, b.\`{{ value_column }}\` FROM date_array a FULL OUTER JOIN raw_events b ON CAST(a.\`{{ timestamp_column }}\` AS LONG) = CAST(b.\`{{ timestamp_column }}\` AS LONG) AND a.\`{{ tagname_column }}\` = b.\`{{ tagname_column }}\`) 
        ,calculation_set_up AS (SELECT \`{{ timestamp_column }}\`, \`Window{{ timestamp_column }}\`, \`{{ tagname_column }}\`, \`{{ value_column }}\`, MOD(\`{{ value_column }}\` - {{ lower_bound }}, ({{ upper_bound }} - {{ lower_bound }}))*(2*pi()/({{ upper_bound }} - {{ lower_bound }})) AS \`{{ value_column }}_in_Radians\`, LAG(\`{{ timestamp_column }}\`) OVER (PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\`) AS \`Previous_{{ timestamp_column }}\`, (unix_millis(\`{{ timestamp_column }}\`) - unix_millis(\`Previous_{{ timestamp_column }}\`)) / 86400000 AS Time_Difference, COS(\`{{ value_column }}_in_Radians\`) AS Cos_Value, SIN(\`{{ value_column }}_in_Radians\`) AS Sin_Value FROM window_events) 
        ,circular_average_calculations AS (SELECT \`Window{{ timestamp_column }}\`, \`{{ tagname_column }}\`, Time_Difference, AVG(Cos_Value) OVER (PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\` ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS Average_Cos, AVG(Sin_Value) OVER (PARTITION BY \`{{ tagname_column }}\` ORDER BY \`{{ timestamp_column }}\` ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS Average_Sin, SQRT(POW(Average_Cos, 2) + POW(Average_Sin, 2)) AS Vector_Length, Average_Cos/Vector_Length AS Rescaled_Average_Cos, Average_Sin/Vector_Length AS Rescaled_Average_Sin, Time_Difference * Rescaled_Average_Cos AS Diff_Average_Cos, Time_Difference * Rescaled_Average_Sin AS Diff_Average_Sin FROM calculation_set_up) 
    `
    let circularStatsQuery = ""
    if(parameters["circular_function"] == "average"){
        circularStatsQuery = `
            ${circularBaseQuery}
            ,circular_average_results AS (SELECT \`Window{{ timestamp_column }}\` AS \`{{ timestamp_column }}\`, \`{{ tagname_column }}\`, sum(Diff_Average_Cos)/sum(Time_Difference) AS Cos_Time_Averages, sum(Diff_Average_Sin)/sum(Time_Difference) AS Sin_Time_Averages, array_min(array(1, sqrt(pow(Cos_Time_Averages, 2) + pow(Sin_Time_Averages, 2)))) AS R, mod(2*pi() + atan2(Sin_Time_Averages, Cos_Time_Averages), 2*pi()) AS Circular_Average_Value_in_Radians, (Circular_Average_Value_in_Radians * ({{ upper_bound }} - {{ lower_bound }})) / (2*pi())+ 0 AS Circular_Average_Value_in_Degrees FROM circular_average_calculations GROUP BY \`{{ tagname_column }}\`, \`Window{{ timestamp_column }}\`) 
            ,project AS (SELECT \`{{ timestamp_column }}\`, \`{{ tagname_column }}\`, Circular_Average_Value_in_Degrees AS \`{{ value_column }}\` FROM circular_average_results) 
            {% if pivot is defined and pivot == true %}
            ,pivot AS (SELECT * FROM project PIVOT (FIRST(\`{{ value_column }}\`) FOR \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}))) 
            SELECT * FROM pivot ORDER BY \`{{ timestamp_column }}\` 
            {% else %}
            SELECT * FROM project ORDER BY \`{{ tagname_column }}\`, \`{{ timestamp_column }}\` 
            {% endif %}
            {% if limit is defined and limit is not none %}
            LIMIT {{ limit }} 
            {% endif %}
            {% if offset is defined and offset is not none %}
            OFFSET {{ offset }} 
            {% endif %}
        `
    } else if(parameters["circular_function"] == "standard_deviation"){
        circularStatsQuery = `
            ${circularBaseQuery} 
            ,circular_average_results AS (SELECT \`Window{{ timestamp_column }}\` AS \`{{ timestamp_column }}\`, \`{{ tagname_column }}\`, sum(Diff_Average_Cos)/sum(Time_Difference) AS Cos_Time_Averages, sum(Diff_Average_Sin)/sum(Time_Difference) AS Sin_Time_Averages, array_min(array(1, sqrt(pow(Cos_Time_Averages, 2) + pow(Sin_Time_Averages, 2)))) AS R, mod(2*pi() + atan2(Sin_Time_Averages, Cos_Time_Averages), 2*pi()) AS Circular_Average_Value_in_Radians, SQRT(-2*LN(R)) * ( {{ upper_bound }} - {{ lower_bound }}) / (2*PI()) AS Circular_Standard_Deviation FROM circular_average_calculations GROUP BY \`{{ tagname_column }}\`, \`Window{{ timestamp_column }}\`) 
            ,project AS (SELECT \`{{ timestamp_column }}\`, \`{{ tagname_column }}\`, Circular_Standard_Deviation AS \`Value\` FROM circular_average_results) 
            {% if pivot is defined and pivot == true %}
            ,pivot AS (SELECT * FROM project PIVOT (FIRST(\`{{ value_column }}\`) FOR \`{{ tagname_column }}\` IN ({{ tag_names | joinWithQuotes }}))) 
            SELECT * FROM pivot ORDER BY \`{{ timestamp_column }}\` 
            {% else %}
            SELECT * FROM project ORDER BY \`{{ tagname_column }}\`, \`{{ timestamp_column }}\` 
            {% endif %}
            {% if limit is defined and limit is not none %}
            LIMIT {{ limit }} 
            {% endif %}
            {% if offset is defined and offset is not none %}
            OFFSET {{ offset }} 
            {% endif %}
        `
    }

    const circularStatsParameters = {
        "source":               parameters["source"],
        "source_metadata":      parameters["source_metadata"],
        "business_unit":        parameters["business_unit"],
        "region":               parameters["region"],
        "asset":                parameters["asset"],
        "data_security_level":  parameters["data_security_level"],
        "data_type":            parameters["data_type"],
        "start_date":           parameters["start_date"],
        "end_date":             parameters["end_date"],
        "tag_names":            [...new Set(parameters["tag_names"])],
        "time_interval_rate":   parameters["time_interval_rate"],
        "time_interval_unit":   parameters["time_interval_unit"],
        "lower_bound":          parameters["lower_bound"],
        "upper_bound":          parameters["upper_bound"],
        "include_bad_data":     parameters["include_bad_data"],
        "time_zone":            parameters["time_zone"],
        "circular_function":    parameters["circular_function"],        
        "pivot":                parameters["pivot"],
        "limit":                parameters["limit"],
        "offset":               parameters["offset"],
        "is_resample":          true,
        "tagname_column":       get(parameters, "tagname_column", "TagName"),
        "timestamp_column":     get(parameters, "timestamp_column", "EventTime"),
        "include_status":       "status_column" in parameters && parameters["status_column"] ? false : true,
        "status_column":        "status_column" in parameters && parameters["status_column"] ? "Status" : get(parameters, "status_column", "Status"),
        "value_column":         get(parameters, "value_column", "Value"),
    }

    return env.renderString(circularStatsQuery, circularStatsParameters)
}


// utils
const get = (obj, key, defaultValue=undefined) => {
    return obj.hasOwnProperty(key) ? obj[key] : defaultValue;
}

function _parse_dates(parameters_dict) {

    let sample_dt;

    if (parameters_dict.hasOwnProperty("start_date")){
        parameters_dict["start_date"] = _parse_date(parameters_dict["start_date"])
        sample_dt = parameters_dict["start_date"]
    }


    if(parameters_dict.hasOwnProperty("end_date")){
        parameters_dict["end_date"] = _parse_date(parameters_dict["end_date"], true)
    }

    if(parameters_dict.hasOwnProperty("timestamps")){
        const parsed_timestamps = parameters_dict["timestamps"].map( dt => _parse_date(dt, false, true))
        parameters_dict["timestamps"] = parsed_timestamps
        sample_dt = parsed_timestamps[0]
    }

    parameters_dict["time_zone"] = moment(sample_dt).format("Z")

    return parameters_dict
}

function _is_date_format(dt, format){
    try{
        return moment(dt, format, true).isValid()
    } catch (error) {
        return false
    }     
}

function _is_star_format(dt){
    const pattern = /^\*([+-]\d+[smhdw])?$/;
    return pattern.test(dt);
}

function _parse_date(dt, is_end_date = false, exclude_date_format = false) {
    if (dt instanceof Date || moment.isMoment(dt)) {
        if (moment(dt).format("HH:mm:ss") === "00:00:00") {
            if (dt.getTimezoneOffset() !== 0) {
                dt = moment(dt).format("YYYY-MM-DDZ");
            } else {
                dt = moment(dt).format("YYYY-MM-DD");
            }
        } else {
            dt = moment(dt).format(TIMESTAMP_FORMAT);
        }
    }

    dt = String(dt);

    if (_is_date_format(dt, "YYYY-MM-DD") && !exclude_date_format) {
        let _time = is_end_date ? "T23:59:59" : "T00:00:00";
        return dt + _time + "+00:00";
    } else if (_is_date_format(dt, "YYYY-MM-DDTHH:mm:ss")) {
        return dt + "+00:00";
    } else if (_is_date_format(dt, TIMESTAMP_FORMAT)) {
        return dt;
    } else if (_is_date_format(dt, "YYYY-MM-DDZ")) {
        let _time = is_end_date ? "T23:59:59" : "T00:00:00";
        dt = dt.substring(0, 10) + _time + dt.substring(10);
        return dt;
    } else if (_is_star_format(dt)){
        return parseStarFormat(dt)
    } else {
        let msg = `Inputted timestamp: '${dt}', is not in the correct format.`;
        if (exclude_date_format) {
            msg += " List of timestamps must be in datetime format.";
        }
        throw new Error(msg);
    }
}


function _convert_to_seconds(s){
    let unit = s.slice(-1);
    let value = parseInt(s.substring(0, s.length - 1), 10);

    return value * seconds_per_unit[unit];
}

function parseStarFormat(dt){
    // parses strings into moment dates that match the pattern  /^\*([+-]\d+[smhdw])?$/;

    // Initialize the date to now
    let date = moment();
    
    // If the input string is more than just "*", parse the offset
    if (dt.length > 1) {
        const operator = dt[1]; // "+" or "-"
        const amount = parseInt(dt.substring(2, dt.length - 1), 10); // Extract the amount as an integer
        const unit = dt[dt.length - 1]; // Extract the unit (s, m, h, or d)
        
        if (operator === '+') {
            date.add(amount, unit);
        } else if (operator === '-') {
            date.subtract(amount, unit);
        }
    }
    
    return date.format(TIMESTAMP_FORMAT);
}