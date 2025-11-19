const database = require('../config/database');

class Device {
  constructor(data = {}) {
    this.id = data.id;
    this.company_id = data.company_id;
    this.hierarchy_id = data.hierarchy_id;
    this.device_type_id = data.device_type_id;
    this.serial_number = data.serial_number;
    this.metadata = data.metadata;
    this.created_at = data.created_at;
    this.device_type_name = data.device_type_name;
    this.hierarchy_name = data.hierarchy_name;
    this.company_name = data.company_name;
  }

  static async findById(id) {
    const query = `
      SELECT d.*, dt.type_name as device_type_name, c.name as company_name, h.name as hierarchy_name
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      JOIN company c ON d.company_id = c.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      WHERE d.id = $1
    `;
    const result = await database.query(query, [id]);
    return result.rows[0] ? new Device(result.rows[0]) : null;
  }

  static async findByCompany(company_id) {
    const query = `
      SELECT d.*, dt.type_name as device_type_name, c.name as company_name, h.name as hierarchy_name
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      JOIN company c ON d.company_id = c.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      WHERE d.company_id = $1
      ORDER BY d.serial_number
    `;
    const result = await database.query(query, [company_id]);
    return result.rows.map(row => new Device(row));
  }

  static async getDeviceChartData(serial_number, timeRange = 'day') {
    let timeFilter = '';
    let groupBy = '';
    
    switch (timeRange) {
      case 'hour':
        timeFilter = "dd.created_at >= now() - interval '1 hour'";
        groupBy = "date_trunc('minute', dd.created_at)";
        break;
      case 'day':
        timeFilter = "dd.created_at >= date_trunc('day', now())";
        groupBy = "date_trunc('minute', dd.created_at)";
        break;
      case 'week':
        timeFilter = "dd.created_at >= now() - interval '7 days'";
        groupBy = "date_trunc('hour', dd.created_at)";
        break;
      case 'month':
        timeFilter = "dd.created_at >= now() - interval '30 days'";
        groupBy = "date_trunc('day', dd.created_at)";
        break;
      default:
        timeFilter = "dd.created_at >= date_trunc('day', now())";
        groupBy = "date_trunc('minute', dd.created_at)";
    }

    // First check what type of device this is
    const deviceTypeQuery = `
      SELECT dt.type_name 
      FROM device d 
      JOIN device_type dt ON d.device_type_id = dt.id 
      WHERE d.serial_number = $1
    `;
    const deviceTypeResult = await database.query(deviceTypeQuery, [serial_number]);
    const deviceType = deviceTypeResult.rows[0]?.type_name;

    // Build dynamic query based on device type
    let selectFields = '';
    if (deviceType === 'MPFM') {
      selectFields = `
        AVG((dd.data->>'GFR')::numeric) AS avg_gfr,
        AVG((dd.data->>'GOR')::numeric) AS avg_gor,
        AVG((dd.data->>'GVF')::numeric) AS avg_gvf,
        AVG((dd.data->>'OFR')::numeric) AS avg_ofr,
        AVG((dd.data->>'WFR')::numeric) AS avg_wfr,
        AVG((dd.data->>'WLR')::numeric) AS avg_wlr,
        AVG((dd.data->>'PressureAvg')::numeric) AS avg_pressure,
        AVG((dd.data->>'TemperatureAvg')::numeric) AS avg_temp
      `;
    } else if (deviceType === 'Pressure Sensor') {
      selectFields = `
        NULL AS avg_gfr,
        NULL AS avg_gor,
        NULL AS avg_gvf,
        NULL AS avg_ofr,
        NULL AS avg_wfr,
        NULL AS avg_wlr,
        AVG((dd.data->>'Pressure')::numeric) AS avg_pressure,
        AVG((dd.data->>'TemperatureAvg')::numeric) AS avg_temp
      `;
    } else if (deviceType === 'Temperature Sensor') {
      selectFields = `
        NULL AS avg_gfr,
        NULL AS avg_gor,
        NULL AS avg_gvf,
        NULL AS avg_ofr,
        NULL AS avg_wfr,
        NULL AS avg_wlr,
        AVG((dd.data->>'PressureAvg')::numeric) AS avg_pressure,
        AVG((dd.data->>'Temperature')::numeric) AS avg_temp
      `;
    } else if (deviceType === 'Flow Meter') {
      selectFields = `
        NULL AS avg_gfr,
        NULL AS avg_gor,
        NULL AS avg_gvf,
        AVG((dd.data->>'FlowRate')::numeric) AS avg_ofr,
        NULL AS avg_wfr,
        NULL AS avg_wlr,
        AVG((dd.data->>'PressureAvg')::numeric) AS avg_pressure,
        AVG((dd.data->>'TemperatureAvg')::numeric) AS avg_temp
      `;
    } else {
      // Default for other device types
      selectFields = `
        NULL AS avg_gfr,
        NULL AS avg_gor,
        NULL AS avg_gvf,
        NULL AS avg_ofr,
        NULL AS avg_wfr,
        NULL AS avg_wlr,
        NULL AS avg_pressure,
        NULL AS avg_temp
      `;
    }

    const query = `
      SELECT 
        ${groupBy} AS time_period,
        ${selectFields},
        COUNT(*) as data_points
      FROM device_data dd
      WHERE dd.serial_number = $1 AND ${timeFilter}
      GROUP BY ${groupBy}
      ORDER BY time_period
    `;

    const result = await database.query(query, [serial_number]);
    return result.rows;
  }

  static async getHierarchyChartData(hierarchy_id, timeRange = 'day') {
    let timeFilter = '';
    let groupBy = '';
    
    switch (timeRange) {
      case 'hour':
        timeFilter = "dd.created_at >= now() - interval '1 hour'";
        groupBy = "date_trunc('minute', dd.created_at)";
        break;
      case 'day':
        timeFilter = "dd.created_at >= date_trunc('day', now())";
        groupBy = "date_trunc('minute', dd.created_at)";
        break;
      case 'week':
        timeFilter = "dd.created_at >= now() - interval '7 days'";
        groupBy = "date_trunc('hour', dd.created_at)";
        break;
      case 'month':
        timeFilter = "dd.created_at >= now() - interval '30 days'";
        groupBy = "date_trunc('day', dd.created_at)";
        break;
      default:
        timeFilter = "dd.created_at >= date_trunc('day', now())";
        groupBy = "date_trunc('minute', dd.created_at)";
    }

    // Primary aggregation from device_data
    const aggQuery = `
WITH RECURSIVE hierarchy_cte AS (
    SELECT id
    FROM hierarchy
    WHERE id = $1
    UNION ALL
    SELECT h.id
    FROM hierarchy h
    JOIN hierarchy_cte c ON h.parent_id = c.id
),
device_data_minute AS (
    SELECT 
      dd.serial_number as device_id,
      ${groupBy} AS minute,
      AVG((dd.data->>'GFR')::numeric) AS avg_gfr,
      AVG((dd.data->>'GOR')::numeric) AS avg_gor,
      AVG((dd.data->>'GVF')::numeric) AS avg_gvf,
      AVG((dd.data->>'OFR')::numeric) AS avg_ofr,
      AVG((dd.data->>'WFR')::numeric) AS avg_wfr,
      AVG((dd.data->>'WLR')::numeric) AS avg_wlr,
      AVG((dd.data->>'PressureAvg')::numeric) AS avg_pressure,
      AVG((dd.data->>'TemperatureAvg')::numeric) AS avg_temp
    FROM device_data dd
    JOIN device d ON d.serial_number= dd.serial_number
    WHERE d.hierarchy_id IN (SELECT id FROM hierarchy_cte) AND ${timeFilter}
    GROUP BY dd.serial_number, ${groupBy}
),
summed AS (
    SELECT 
      minute,
      SUM(avg_gfr) AS total_gfr,
      SUM(avg_gor) AS total_gor,
      SUM(avg_ofr) AS total_ofr,
      SUM(avg_wfr) AS total_wfr,
      CASE 
        WHEN COALESCE(SUM(avg_gfr), 0) + COALESCE(SUM(avg_ofr), 0) + COALESCE(SUM(avg_wfr), 0) > 0 
        THEN COALESCE(SUM(avg_gfr), 0) * 100.0 / (COALESCE(SUM(avg_gfr), 0) + COALESCE(SUM(avg_ofr), 0) + COALESCE(SUM(avg_wfr), 0))
        ELSE 0 
      END AS total_gvf,
      CASE 
        WHEN COALESCE(SUM(avg_ofr), 0) + COALESCE(SUM(avg_wfr), 0) > 0 
        THEN COALESCE(SUM(avg_wfr), 0) * 100.0 / (COALESCE(SUM(avg_ofr), 0) + COALESCE(SUM(avg_wfr), 0))
        ELSE 0 
      END AS total_wlr,
      AVG(avg_pressure) AS avg_pressure,
      AVG(avg_temp) AS avg_temp,
      COUNT(DISTINCT device_id) as device_count
    FROM device_data_minute
    GROUP BY minute
)
SELECT * 
FROM summed
ORDER BY minute
    `;

    const result = await database.query(aggQuery, [hierarchy_id]);

    if (result.rows && result.rows.length > 0) {
      return result.rows; // real historical data exists — return it
    }

    // --- Multi-point synthetic fallback (10 points, 1-minute interval) ---
    // Uses device_latest values but repeats them across a 10-minute series so front-end can draw a line.
    const fallbackQuery = `
WITH RECURSIVE hierarchy_cte AS (
    SELECT id FROM hierarchy WHERE id = $1
    UNION ALL
    SELECT h.id FROM hierarchy h JOIN hierarchy_cte c ON h.parent_id = c.id
),
devs AS (
    SELECT serial_number FROM device WHERE hierarchy_id IN (SELECT id FROM hierarchy_cte)
),
latest AS (
    SELECT dl.serial_number,
           (dl.data->>'GFR')::numeric AS gfr,
           (dl.data->>'GOR')::numeric AS gor,
           (dl.data->>'OFR')::numeric AS ofr,
           (dl.data->>'WFR')::numeric AS wfr,
           (dl.data->>'GVF')::numeric AS gvf,
           (dl.data->>'WLR')::numeric AS wlr,
           (dl.data->>'PressureAvg')::numeric AS pressure,
           (dl.data->>'TemperatureAvg')::numeric AS temp
    FROM device_latest dl
    WHERE dl.serial_number IN (SELECT serial_number FROM devs)
)
SELECT
  gs AS minute,
  COALESCE(SUM(latest.gfr),0) AS total_gfr,
  COALESCE(SUM(latest.gor),0) AS total_gor,
  COALESCE(SUM(latest.ofr),0) AS total_ofr,
  COALESCE(SUM(latest.wfr),0) AS total_wfr,
  CASE
    WHEN COALESCE(SUM(latest.gfr),0) + COALESCE(SUM(latest.ofr),0) + COALESCE(SUM(latest.wfr),0) > 0
    THEN COALESCE(SUM(latest.gfr),0) * 100.0 / (COALESCE(SUM(latest.gfr),0) + COALESCE(SUM(latest.ofr),0) + COALESCE(SUM(latest.wfr),0))
    ELSE 0
  END AS total_gvf,
  CASE
    WHEN COALESCE(SUM(latest.ofr),0) + COALESCE(SUM(latest.wfr),0) > 0
    THEN COALESCE(SUM(latest.wfr),0) * 100.0 / (COALESCE(SUM(latest.ofr),0) + COALESCE(SUM(latest.wfr),0))
    ELSE 0
  END AS total_wlr,
  AVG(latest.pressure) AS avg_pressure,
  AVG(latest.temp) AS avg_temp,
  COUNT(latest.serial_number) AS device_count
FROM generate_series(now() - interval '9 minutes', now(), interval '1 minute') AS gs
LEFT JOIN latest ON true
GROUP BY gs
ORDER BY gs;
    `;

    const fallbackResult = await database.query(fallbackQuery, [hierarchy_id]);
    return fallbackResult.rows || [];
  }

  static async getLatestDeviceData(serial_number) {
    const query = `
      SELECT 
        dd.*,
        d.serial_number,
        dt.type_name as device_type
      FROM device_data dd
      JOIN device d ON dd.serial_number = d.serial_number
      JOIN device_type dt ON d.device_type_id = dt.id
      WHERE dd.serial_number = $1
      ORDER BY dd.created_at DESC
      LIMIT 1
    `;

    const result = await database.query(query, [serial_number]);
    return result.rows[0] || null;
  }

  // Debug method to check what devices are found for a hierarchy
  static async getDevicesForHierarchy(hierarchy_id) {
    const query = `
      WITH RECURSIVE hierarchy_cte AS (
        SELECT id, name, level_id
        FROM hierarchy
        WHERE id = $1

        UNION ALL


        SELECT h.id, h.name, h.level_id
        FROM hierarchy h
        JOIN hierarchy_cte c ON h.parent_id = c.id
      )
      SELECT 
        h.id as hierarchy_id,
        h.name as hierarchy_name,
        hl.name as level_name,
        d.id as device_id,
        d.serial_number,
        dt.type_name,
        COUNT(dd.id) as data_count
      FROM hierarchy_cte h
      JOIN hierarchy_level hl ON h.level_id = hl.id
      LEFT JOIN device d ON h.id = d.hierarchy_id
      LEFT JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN device_data dd ON d.serial_number= dd.serial_number AND dd.created_at >= date_trunc('day', now())
      GROUP BY h.id, h.name, hl.name, d.id, d.serial_number, dt.type_name
      ORDER BY h.id, d.serial_number
    `;

    const result = await database.query(query, [hierarchy_id]);
    return result.rows;
  }
  toJSON() {
    return {
      id: this.id,
      company_id: this.company_id,
      hierarchy_id: this.hierarchy_id,
      device_type_id: this.device_type_id,
      serial_number: this.serial_number,
      metadata: this.metadata,
      created_at: this.created_at,
      device_type_name: this.device_type_name,
      hierarchy_name: this.hierarchy_name,
      company_name: this.company_name
    };
  }
}

module.exports = Device;
