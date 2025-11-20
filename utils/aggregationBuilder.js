const { extractDependentVariables, buildPostAggregationExpression } = require('./expressionBuilder');

const getTimeBucket = (timeRange) => {
  switch (timeRange) {
    case 'today':
      return "date_trunc('minute', dd.created_at)";
    case 'week':
      return "date_trunc('hour', dd.created_at)";
    case 'month':
      return "date_trunc('day', dd.created_at)";
    default:
      return "date_trunc('minute', dd.created_at)";
  }
};

const getTimeFilter = (timeRange) => {
  switch (timeRange) {
    case 'today':
      return "AND dd.created_at >= DATE_TRUNC('day', NOW())";
    case 'week':
      return "AND dd.created_at >= DATE_TRUNC('day', NOW()) - INTERVAL '7 days'";
    case 'month':
      return "AND dd.created_at >= DATE_TRUNC('day', NOW()) - INTERVAL '30 days'";
    default:
      return "AND dd.created_at >= DATE_TRUNC('day', NOW()) - INTERVAL '1 day'";
  }
};

const buildHistoricalAggregationQuery = (
  companyId,
  deviceTypeId,
  variableTag,
  variableName,
  expression,
  requiresPostAgg,
  timeRange,
  hierarchyId,
  deviceId,
  limit
) => {
  const timeBucket = getTimeBucket(timeRange);
  const timeFilter = getTimeFilter(timeRange);
  const hasExpression = expression && expression.trim();

  let deviceFilterJoin = '';
  let deviceFilterWhere = '';
  let paramCount = 3;

  if (hierarchyId) {
    deviceFilterJoin = `
      WITH RECURSIVE hierarchy_tree AS (
        SELECT id FROM hierarchy WHERE id = $4
        UNION ALL
        SELECT h.id FROM hierarchy h
        JOIN hierarchy_tree ht ON h.parent_id = ht.id
      )
    `;
    deviceFilterWhere = `AND d.hierarchy_id IN (SELECT id FROM hierarchy_tree)`;
    paramCount = 5;
  } else if (deviceId) {
    deviceFilterWhere = `AND d.id = $4`;
    paramCount = 5;
  }

  let query;

  if (hasExpression && requiresPostAgg) {
    const dependentVars = extractDependentVariables(expression);
    const selectFields = dependentVars
      .map(v => `AVG((dd.data->>'${v}')::numeric) AS ${v}`)
      .join(',\n        ');

    const withPrefix = deviceFilterJoin && deviceFilterJoin.trim()
      ? `${deviceFilterJoin.trim().endsWith(')') ? deviceFilterJoin.trim() + ',' : deviceFilterJoin.trim() + ','}`
      : 'WITH';

    query = `
      ${withPrefix}
      device_data_minute AS (
        SELECT
          dd.serial_number as device_id,
          ${timeBucket} AS time_bucket,
          ${selectFields}
        FROM device_data dd
        INNER JOIN device d ON dd.device_id = d.id
        WHERE d.company_id = $1
          AND d.device_type_id = $2
          ${deviceFilterWhere}
          ${timeFilter}
        GROUP BY dd.serial_number, ${timeBucket}
      ),
      summed AS (
        SELECT
          time_bucket,
          ${dependentVars.map(v => `SUM(${v}) AS ${v}`).join(',\n          ')}
        FROM device_data_minute
        GROUP BY time_bucket
      )
      SELECT
        time_bucket as timestamp,
        (${buildPostAggregationExpression(expression)}) as value
      FROM summed
      ORDER BY time_bucket ASC
      LIMIT $3
    `;

    const params = hierarchyId || deviceId
      ? [companyId, deviceTypeId, parseInt(limit), parseInt(hierarchyId || deviceId)]
      : [companyId, deviceTypeId, parseInt(limit)];

    return { query, params };
  }

  if (hasExpression) {
    const sqlExpression = buildExpressionQuery(expression, 'dd.data');
    query = `
      ${deviceFilterJoin}
      SELECT
        ${timeBucket} as timestamp,
        AVG((${sqlExpression})) as value
      FROM device_data dd
      INNER JOIN device d ON dd.device_id = d.id
      WHERE d.company_id = $1
        AND d.device_type_id = $2
        ${deviceFilterWhere}
        ${timeFilter}
      GROUP BY ${timeBucket}
      ORDER BY timestamp ASC
      LIMIT $3
    `;

    const params = hierarchyId || deviceId
      ? [companyId, deviceTypeId, parseInt(limit), parseInt(hierarchyId || deviceId)]
      : [companyId, deviceTypeId, parseInt(limit)];

    return { query, params };
  }

  query = `
    ${deviceFilterJoin}
    SELECT
      ${timeBucket} as timestamp,
      AVG((dd.data->>'${variableTag}')::numeric) as device_avg,
      SUM((dd.data->>'${variableTag}')::numeric) as value
    FROM device_data dd
    INNER JOIN device d ON dd.device_id = d.id
    WHERE d.company_id = $1
      AND d.device_type_id = $2
      ${deviceFilterWhere}
      ${timeFilter}
      AND dd.data ? '${variableTag}'
    GROUP BY ${timeBucket}
    ORDER BY timestamp ASC
    LIMIT $3
  `;

  const params = hierarchyId || deviceId
    ? [companyId, deviceTypeId, parseInt(limit), parseInt(hierarchyId || deviceId)]
    : [companyId, deviceTypeId, parseInt(limit)];

  return { query, params };
};

const buildRealtimeAggregationQuery = (
  companyId,
  deviceTypeId,
  variableTag,
  expression,
  requiresPostAgg,
  aggregationMethod,
  hierarchyId,
  deviceId
) => {
  const hasExpression = expression && expression.trim();

  let deviceFilterJoin = '';
  let deviceFilterWhere = '';

  if (hierarchyId) {
    deviceFilterJoin = `
      WITH RECURSIVE hierarchy_tree AS (
        SELECT id FROM hierarchy WHERE id = $3
        UNION ALL
        SELECT h.id FROM hierarchy h
        JOIN hierarchy_tree ht ON h.parent_id = ht.id
      )
    `;
    deviceFilterWhere = `AND d.hierarchy_id IN (SELECT id FROM hierarchy_tree)`;
  } else if (deviceId) {
    deviceFilterWhere = `AND d.id = $3`;
  }

  let query;

  if (hasExpression && requiresPostAgg) {
    const dependentVars = extractDependentVariables(expression);
    const selectFields = dependentVars
      .map(v => `(dl.data->>'${v}')::numeric AS ${v}`)
      .join(',\n        ');

    const withPrefix = deviceFilterJoin && deviceFilterJoin.trim()
      ? `${deviceFilterJoin.trim().endsWith(')') ? deviceFilterJoin.trim() + ',' : deviceFilterJoin.trim() + ','}`
      : 'WITH';

    const postAggExpr = buildPostAggregationExpression(expression);

    query = `
      ${withPrefix}
      aggregated_data AS (
        SELECT
          ${selectFields},
          dl.updated_at,
          dl.serial_number
        FROM device_latest dl
        INNER JOIN device d ON dl.device_id = d.id
        WHERE d.company_id = $1
          AND d.device_type_id = $2
          ${deviceFilterWhere}
      ),
      summed AS (
        SELECT
          ${dependentVars.map(v => `COALESCE(SUM(${v}), 0) AS ${v}`).join(',\n          ')},
          MAX(updated_at) as timestamp,
          COUNT(*) as device_count
        FROM aggregated_data
      )
      SELECT
        (${postAggExpr})::text as value,
        timestamp,
        device_count
      FROM summed
    `;
  } else if (hasExpression) {
    const sqlExpression = buildExpressionQuery(expression, 'dl.data');

    query = `
      ${deviceFilterJoin}
      SELECT
        (${sqlExpression})::text as value,
        dl.updated_at as timestamp
      FROM device_latest dl
      INNER JOIN device d ON dl.device_id = d.id
      WHERE d.company_id = $1
        AND d.device_type_id = $2
        ${deviceFilterWhere}
    `;
  } else {
    const aggregateFunc = aggregationMethod === 'sum' ? 'SUM' : aggregationMethod === 'avg' ? 'AVG' : 'SUM';
    const selectExpr = aggregationMethod === 'percentage'
      ? `ROUND(100.0 * SUM((dl.data->>'${variableTag}')::numeric) / NULLIF(COUNT(*), 0), 2)::text`
      : `${aggregateFunc}((dl.data->>'${variableTag}')::numeric)::text`;

    query = `
      ${deviceFilterJoin}
      SELECT
        ${selectExpr} as value,
        MAX(dl.updated_at) as timestamp,
        COUNT(*) as device_count
      FROM device_latest dl
      INNER JOIN device d ON dl.device_id = d.id
      WHERE d.company_id = $1
        AND d.device_type_id = $2
        ${deviceFilterWhere}
        AND dl.data ? '${variableTag}'
      GROUP BY d.company_id
    `;
  }

  const params = hierarchyId || deviceId
    ? [companyId, deviceTypeId, parseInt(hierarchyId || deviceId)]
    : [companyId, deviceTypeId];

  return { query, params };
};

const { buildExpressionQuery } = require('./expressionBuilder');

module.exports = {
  getTimeBucket,
  getTimeFilter,
  buildHistoricalAggregationQuery,
  buildRealtimeAggregationQuery
};
