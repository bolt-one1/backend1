const database = require('../config/database');

class DeviceAlarm {
  constructor(data = {}) {
    this.id = data.id;
    this.device_serial = data.device_serial;
    this.alarm_type_id = data.alarm_type_id;
    this.created_at = data.created_at;
    this.updated_at = data.updated_at;
    this.status_id = data.status_id;
    this.acknowledged_by = data.acknowledged_by;
    this.acknowledged_at = data.acknowledged_at;
    this.resolved_by = data.resolved_by;
    this.resolved_at = data.resolved_at;
    this.message = data.message;
    this.metadata = data.metadata;
    
    // Joined fields
    this.alarm_type_name = data.alarm_type_name;
    this.alarm_severity = data.alarm_severity;
    this.status_name = data.status_name;
    this.device_id = data.device_id;
    this.device_type_name = data.device_type_name;
    this.hierarchy_name = data.hierarchy_name;
    this.company_id = data.company_id;
    this.company_name = data.company_name;
  }

  static async findById(id) {
    const query = `
      SELECT 
        da.*,
        at.name as alarm_type_name,
        at.severity as alarm_severity,
        ast.name as status_name,
        d.id as device_id,
        dt.type_name as device_type_name,
        h.name as hierarchy_name,
        c.id as company_id,
        c.name as company_name
      FROM device_alarms da
      JOIN alarm_types at ON da.alarm_type_id = at.id
      JOIN alarm_status_type ast ON da.status_id = ast.id
      LEFT JOIN device d ON da.device_serial = d.serial_number
      LEFT JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      LEFT JOIN company c ON d.company_id = c.id
      WHERE da.id = $1
    `;
    
    const result = await database.query(query, [id]);
    return result.rows[0] ? new DeviceAlarm(result.rows[0]) : null;
  }

  static async findByCompany(company_id, filters = {}) {
    const {
      hierarchy_id,
      device_serial,
      alarm_type_id,
      status_id,
      severity,
      page = 1,
      limit = 20,
      sort_by = 'created_at',
      sort_order = 'DESC'
    } = filters;

    const offset = (page - 1) * limit;
    let whereConditions = ['c.id = $1'];
    let queryParams = [company_id];
    let paramIndex = 2;

    // Build dynamic WHERE conditions
    if (hierarchy_id) {
      whereConditions.push(`d.hierarchy_id = $${paramIndex}`);
      queryParams.push(hierarchy_id);
      paramIndex++;
    }

    if (device_serial) {
      whereConditions.push(`da.device_serial ILIKE $${paramIndex}`);
      queryParams.push(`%${device_serial}%`);
      paramIndex++;
    }

    if (alarm_type_id) {
      whereConditions.push(`da.alarm_type_id = $${paramIndex}`);
      queryParams.push(alarm_type_id);
      paramIndex++;
    }

    if (status_id) {
      whereConditions.push(`da.status_id = $${paramIndex}`);
      queryParams.push(status_id);
      paramIndex++;
    }

    if (severity) {
      whereConditions.push(`at.severity = $${paramIndex}`);
      queryParams.push(severity);
      paramIndex++;
    }

    const query = `
      SELECT 
        da.*,
        at.name as alarm_type_name,
        at.severity as alarm_severity,
        ast.name as status_name,
        d.id as device_id,
        dt.type_name as device_type_name,
        h.name as hierarchy_name,
        c.id as company_id,
        c.name as company_name
      FROM device_alarms da
      JOIN alarm_types at ON da.alarm_type_id = at.id
      JOIN alarm_status_type ast ON da.status_id = ast.id
      LEFT JOIN device d ON da.device_serial = d.serial_number AND d.company_id = $1
      LEFT JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      LEFT JOIN company c ON d.company_id = c.id
      WHERE ${whereConditions.join(' AND ')}
      ORDER BY da.${sort_by} ${sort_order}
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;

    queryParams.push(limit, offset);
    const result = await database.query(query, queryParams);

    // Get total count for pagination
    const countQuery = `
      SELECT COUNT(DISTINCT da.id) as total
      FROM device_alarms da
      JOIN alarm_types at ON da.alarm_type_id = at.id
      JOIN alarm_status_type ast ON da.status_id = ast.id
      LEFT JOIN device d ON da.device_serial = d.serial_number AND d.company_id = $1
      LEFT JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      LEFT JOIN company c ON d.company_id = c.id
      WHERE ${whereConditions.join(' AND ')}
    `;

    const countResult = await database.query(countQuery, queryParams.slice(0, -2));
    const total = parseInt(countResult.rows[0].total);

    return {
      alarms: result.rows.map(row => new DeviceAlarm(row)),
      pagination: {
        page,
        limit,
        total,
        pages: Math.ceil(total / limit)
      }
    };
  }

  static async findByHierarchy(hierarchy_id, company_id, filters = {}) {
    const {
      device_serial,
      alarm_type_id,
      status_id,
      severity,
      page = 1,
      limit = 20,
      sort_by = 'created_at',
      sort_order = 'DESC'
    } = filters;

    const offset = (page - 1) * limit;
    let whereConditions = ['c.id = $1'];
    let queryParams = [company_id];
    let paramIndex = 2;

    // Add hierarchy filter using recursive CTE
    const hierarchyFilter = `
      WITH RECURSIVE hierarchy_cte AS (
        SELECT id FROM hierarchy WHERE id = $${paramIndex}
        UNION ALL
        SELECT h.id FROM hierarchy h JOIN hierarchy_cte c ON h.parent_id = c.id
      )
    `;
    
    whereConditions.push(`d.hierarchy_id IN (SELECT id FROM hierarchy_cte)`);
    queryParams.push(hierarchy_id);
    paramIndex++;

    // Build additional WHERE conditions
    if (device_serial) {
      whereConditions.push(`da.device_serial ILIKE $${paramIndex}`);
      queryParams.push(`%${device_serial}%`);
      paramIndex++;
    }

    if (alarm_type_id) {
      whereConditions.push(`da.alarm_type_id = $${paramIndex}`);
      queryParams.push(alarm_type_id);
      paramIndex++;
    }

    if (status_id) {
      whereConditions.push(`da.status_id = $${paramIndex}`);
      queryParams.push(status_id);
      paramIndex++;
    }

    if (severity) {
      whereConditions.push(`at.severity = $${paramIndex}`);
      queryParams.push(severity);
      paramIndex++;
    }

    const query = `
      ${hierarchyFilter}
      SELECT 
        da.*,
        at.name as alarm_type_name,
        at.severity as alarm_severity,
        ast.name as status_name,
        d.id as device_id,
        dt.type_name as device_type_name,
        h.name as hierarchy_name,
        c.id as company_id,
        c.name as company_name
      FROM device_alarms da
      JOIN alarm_types at ON da.alarm_type_id = at.id
      JOIN alarm_status_type ast ON da.status_id = ast.id
      LEFT JOIN device d ON da.device_serial = d.serial_number
      LEFT JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      LEFT JOIN company c ON d.company_id = c.id
      WHERE ${whereConditions.join(' AND ')}
      ORDER BY da.${sort_by} ${sort_order}
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `;

    queryParams.push(limit, offset);
    const result = await database.query(query, queryParams);

    // Get total count
    const countQuery = `
      ${hierarchyFilter}
      SELECT COUNT(DISTINCT da.id) as total
      FROM device_alarms da
      JOIN alarm_types at ON da.alarm_type_id = at.id
      JOIN alarm_status_type ast ON da.status_id = ast.id
      LEFT JOIN device d ON da.device_serial = d.serial_number
      LEFT JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      LEFT JOIN company c ON d.company_id = c.id
      WHERE ${whereConditions.join(' AND ')}
    `;

    const countResult = await database.query(countQuery, queryParams.slice(0, -2));
    const total = parseInt(countResult.rows[0].total);

    return {
      alarms: result.rows.map(row => new DeviceAlarm(row)),
      pagination: {
        page,
        limit,
        total,
        pages: Math.ceil(total / limit)
      }
    };
  }

  static async create(alarmData) {
    const { 
      device_serial, 
      alarm_type_id, 
      message, 
      metadata = {},
      status_id = 1 // Default to 'Active' status
    } = alarmData;
    
    const query = `
      INSERT INTO device_alarms (device_serial, alarm_type_id, message, metadata, status_id, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, now(), now())
      RETURNING *
    `;
    
    const result = await database.query(query, [
      device_serial, 
      alarm_type_id, 
      message, 
      JSON.stringify(metadata),
      status_id
    ]);
    
    return new DeviceAlarm(result.rows[0]);
  }

  static async updateStatus(id, status_id, user_id = null) {
    let query, params;
    
    if (status_id === 2) { // Acknowledged
      query = `
        UPDATE device_alarms 
        SET status_id = $2, acknowledged_by = $3, acknowledged_at = now(), updated_at = now()
        WHERE id = $1
        RETURNING *
      `;
      params = [id, status_id, user_id];
    } else if (status_id === 3) { // Resolved
      query = `
        UPDATE device_alarms 
        SET status_id = $2, resolved_by = $3, resolved_at = now(), updated_at = now()
        WHERE id = $1
        RETURNING *
      `;
      params = [id, status_id, user_id];
    } else {
      query = `
        UPDATE device_alarms 
        SET status_id = $2, updated_at = now()
        WHERE id = $1
        RETURNING *
      `;
      params = [id, status_id];
    }
    
    const result = await database.query(query, params);
    return result.rows[0] ? new DeviceAlarm(result.rows[0]) : null;
  }

  static async getAlarmStatistics(company_id, hierarchy_id = null) {
    let whereCondition = 'c.id = $1';
    let queryParams = [company_id];
    let hierarchyJoin = '';
    
    if (hierarchy_id) {
      hierarchyJoin = `
        WITH RECURSIVE hierarchy_cte AS (
          SELECT id FROM hierarchy WHERE id = $2
          UNION ALL
          SELECT h.id FROM hierarchy h JOIN hierarchy_cte hc ON h.parent_id = hc.id
        )
      `;
      whereCondition += ' AND h.id IN (SELECT id FROM hierarchy_cte)';
      queryParams.push(hierarchy_id);
    }

    const query = `
      ${hierarchyJoin}
      SELECT 
        COUNT(*) as total_alarms,
        COUNT(CASE WHEN ast.name = 'Active' THEN 1 END) as active_alarms,
        COUNT(CASE WHEN ast.name = 'Acknowledged' THEN 1 END) as acknowledged_alarms,
        COUNT(CASE WHEN ast.name = 'Resolved' THEN 1 END) as resolved_alarms,
        COUNT(CASE WHEN at.severity = 'Critical' THEN 1 END) as critical_alarms,
        COUNT(CASE WHEN at.severity = 'Major' THEN 1 END) as major_alarms,
        COUNT(CASE WHEN at.severity = 'Minor' THEN 1 END) as minor_alarms,
        COUNT(CASE WHEN at.severity = 'Warning' THEN 1 END) as warning_alarms
      FROM device_alarms da
      JOIN alarm_types at ON da.alarm_type_id = at.id
      JOIN alarm_status_type ast ON da.status_id = ast.id
      LEFT JOIN device d ON da.device_serial = d.serial_number
      LEFT JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      LEFT JOIN company c ON d.company_id = c.id
      WHERE ${whereCondition}
    `;

    const result = await database.query(query, queryParams);
    return result.rows[0];
  }

  toJSON() {
    return {
      id: this.id,
      device_serial: this.device_serial,
      device_id: this.device_id,
      device_type: this.device_type_name,
      hierarchy_name: this.hierarchy_name,
      alarm_type: {
        id: this.alarm_type_id,
        name: this.alarm_type_name,
        severity: this.alarm_severity
      },
      status: {
        id: this.status_id,
        name: this.status_name
      },
      message: this.message,
      metadata: this.metadata,
      created_at: this.created_at,
      updated_at: this.updated_at,
      acknowledged_by: this.acknowledged_by,
      acknowledged_at: this.acknowledged_at,
      resolved_by: this.resolved_by,
      resolved_at: this.resolved_at,
      company: {
        id: this.company_id,
        name: this.company_name
      }
    };
  }
}

module.exports = DeviceAlarm;