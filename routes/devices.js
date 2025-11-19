const express = require('express');
const { protect } = require('../middleware/auth');
const database = require('../config/database');

const router = express.Router();

// @desc    Get devices for a specific hierarchy (region/area/field/well)
// @route   GET /api/devices/hierarchy/:hierarchyId
// @access  Private
router.get('/hierarchy/:hierarchyId', protect, async (req, res) => {
  try {
    const hierarchyId = parseInt(req.params.hierarchyId);
    const search = req.query.search || '';
    const status = req.query.status || 'all'; // all, online, offline
    const deviceType = req.query.deviceType || 'all';

    // Validate hierarchyId
    if (!hierarchyId || isNaN(hierarchyId)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid hierarchy ID provided'
      });
    }

    // Check if hierarchy exists and user has access
    const hierarchyCheck = await database.query(`
      SELECT h.*, c.id as company_id, c.name as company_name
      FROM hierarchy h
      JOIN company c ON h.company_id = c.id
      WHERE h.id = $1
    `, [hierarchyId]);

    if (hierarchyCheck.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Hierarchy not found'
      });
    }

    const hierarchy = hierarchyCheck.rows[0];

    // Check if user has access to this hierarchy
    if (req.user.role !== 'admin' && hierarchy.company_id !== req.user.company_id) {
      return res.status(403).json({
        success: false,
        message: 'Access denied to this hierarchy'
      });
    }

    // Build the main query with your provided structure plus device name
    let query = `
      WITH RECURSIVE hierarchy_cte AS (
        SELECT id, name
        FROM hierarchy
        WHERE id = $1
        UNION ALL
        SELECT h.id, h.name
        FROM hierarchy h
        JOIN hierarchy_cte c ON h.parent_id = c.id
      ),
      devices AS (
        SELECT 
          d.id, 
          d.hierarchy_id, 
          d.serial_number AS device_serial, 
          h.name AS hierarchy_name,
          dt.type_name AS device_name,
          dt.logo AS device_logo,
          d.metadata
        FROM device d
        JOIN hierarchy h ON h.id = d.hierarchy_id
        JOIN device_type dt ON d.device_type_id = dt.id
        WHERE d.hierarchy_id IN (SELECT id FROM hierarchy_cte)
      )
      SELECT 
        d.id AS device_id,
        d.device_serial,
        d.device_name,
        d.device_logo,
        d.hierarchy_name,
        d.metadata,
        l.longitude,
        l.latitude,
        l.updated_at AS last_comm_time,
          CASE 
          WHEN l.updated_at >= now() - interval '5 minutes' THEN 'Online'
          ELSE 'Offline'
        END AS status,
        COALESCE((l.data->>'GFR')::numeric, 0) AS gfr,
        COALESCE((l.data->>'GOR')::numeric, 0) AS gor,
        COALESCE((l.data->>'OFR')::numeric, 0) AS ofr,
        COALESCE((l.data->>'WFR')::numeric, 0) AS wfr,
        COALESCE((l.data->>'GVF')::numeric, 0) AS gvf,
        COALESCE((l.data->>'WLR')::numeric, 0) AS wlr,
        COALESCE((l.data->>'PressureAvg')::numeric, 0) AS pressure,
        COALESCE((l.data->>'TemperatureAvg')::numeric, 0) AS temperature
      FROM devices d
      LEFT JOIN device_latest l ON l.serial_number = d.device_serial
    `;

    const queryParams = [hierarchyId];
    let paramIndex = 2;

    // Add search filter
    if (search) {
      query += ` WHERE (d.device_serial ILIKE $${paramIndex} OR d.device_name ILIKE $${paramIndex} OR d.hierarchy_name ILIKE $${paramIndex})`;
      queryParams.push(`%${search}%`);
      paramIndex++;
    }

    // Add status filter
    if (status !== 'all') {
      const statusCondition = status === 'online' 
               ? `l.updated_at >= now() - interval '5 minutes'`
        : `(l.updated_at IS NULL OR l.updated_at < now() - interval '5 minutes')`;
      
      query += search ? ` AND ${statusCondition}` : ` WHERE ${statusCondition}`;
    }

    // Add device type filter
    if (deviceType !== 'all') {
      const deviceTypeCondition = `d.device_name = $${paramIndex}`;
      query += (search || status !== 'all') ? ` AND ${deviceTypeCondition}` : ` WHERE ${deviceTypeCondition}`;
      queryParams.push(deviceType);
      paramIndex++;
    }

    query += ` ORDER BY d.hierarchy_name, d.device_name, d.device_serial`;

    const result = await database.query(query, queryParams);

    // Get summary statistics
    const statsQuery = `
      WITH RECURSIVE hierarchy_cte AS (
        SELECT id, name
        FROM hierarchy
        WHERE id = $1
        UNION ALL
        SELECT h.id, h.name
        FROM hierarchy h
        JOIN hierarchy_cte c ON h.parent_id = c.id
      )
      SELECT 
        COUNT(DISTINCT d.id) as total_devices,
        COUNT(DISTINCT CASE WHEN l.updated_at >= now() - interval '5 minutes' THEN d.id END) as online_devices,
        (SELECT COUNT(*) FROM device_alarms da 
         JOIN device dev ON da.device_serial = dev.serial_number 
         WHERE dev.hierarchy_id IN (SELECT id FROM hierarchy_cte)) as total_alarms,
        COUNT(DISTINCT d.hierarchy_id) as locations
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN device_latest l ON l.serial_number = d.serial_number
      WHERE d.hierarchy_id IN (SELECT id FROM hierarchy_cte)
    `;

    const statsResult = await database.query(statsQuery, [hierarchyId]);
    const stats = statsResult.rows[0];

    // Get device types for filtering
    const deviceTypesQuery = `
      WITH RECURSIVE hierarchy_cte AS (
        SELECT id, name
        FROM hierarchy
        WHERE id = $1
        UNION ALL
        SELECT h.id, h.name
        FROM hierarchy h
        JOIN hierarchy_cte c ON h.parent_id = c.id
      )
      SELECT DISTINCT dt.type_name
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      WHERE d.hierarchy_id IN (SELECT id FROM hierarchy_cte)
      ORDER BY dt.type_name
    `;

    const deviceTypesResult = await database.query(deviceTypesQuery, [hierarchyId]);

    res.json({
      success: true,
      message: 'Devices retrieved successfully',
      data: {
        hierarchy: {
          id: hierarchy.id,
          name: hierarchy.name,
          company: hierarchy.company_name
        },
        devices: result.rows.map(row => ({
          deviceId: row.device_id,
          deviceSerial: row.device_serial,
          deviceName: row.device_name,
          deviceLogo: row.device_logo,
          wellName: row.hierarchy_name,
          metadata: row.metadata,
          location: {
            longitude: row.longitude,
            latitude: row.latitude
          },
          lastCommTime: row.last_comm_time,
          status: row.status,
          flowData: {
            gfr: parseFloat(row.gfr) || 0,
            gor: parseFloat(row.gor) || 0,
            ofr: parseFloat(row.ofr) || 0,
            wfr: parseFloat(row.wfr) || 0,
            gvf: parseFloat(row.gvf) || 0,
            wlr: parseFloat(row.wlr) || 0,
            pressure: parseFloat(row.pressure) || 0,
            temperature: parseFloat(row.temperature) || 0
          }
        })),
        statistics: {
          totalDevices: parseInt(stats.total_devices) || 0,
          onlineDevices: parseInt(stats.online_devices) || 0,
          offlineDevices: (parseInt(stats.total_devices) || 0) - (parseInt(stats.online_devices) || 0),
          totalAlarms: parseInt(stats.total_alarms) || 0,
          locations: parseInt(stats.locations) || 0
        },
        filters: {
          availableDeviceTypes: deviceTypesResult.rows.map(row => row.type_name),
          currentFilters: {
            search,
            status,
            deviceType
          }
        }
      }
    });
  } catch (error) {
    console.error('Get devices by hierarchy error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting devices'
    });
  }
});

// @desc    Get all devices for user's company
// @route   GET /api/devices
// @access  Private
router.get('/', protect, async (req, res) => {
  try {
    let company_id = req.user.company_id;
    const search = req.query.search || '';
    const status = req.query.status || 'all';
    const deviceType = req.query.deviceType || 'all';
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;

    // If admin requests specific company devices
    if (req.query.company_id && req.user.role === 'admin') {
      company_id = parseInt(req.query.company_id);
    }

    let query = `
      SELECT 
        d.id AS device_id,
        d.serial_number AS device_serial,
        dt.type_name AS device_name,
        dt.logo AS device_logo,
        h.name AS hierarchy_name,
        hl.name AS hierarchy_level,
        d.metadata,
        l.longitude,
        l.latitude,
        l.updated_at AS last_comm_time,
        CASE 
          WHEN l.updated_at >= now() - interval '5 minutes' THEN 'Online'
          ELSE 'Offline'
        END AS status,
        COALESCE((l.data->>'GFR')::numeric, 0) AS gfr,
        COALESCE((l.data->>'GOR')::numeric, 0) AS gor,
        COALESCE((l.data->>'OFR')::numeric, 0) AS ofr,
        COALESCE((l.data->>'WFR')::numeric, 0) AS wfr,
        COALESCE((l.data->>'GVF')::numeric, 0) AS gvf,
        COALESCE((l.data->>'WLR')::numeric, 0) AS wlr,
        COALESCE((l.data->>'PressureAvg')::numeric, 0) AS pressure,
        COALESCE((l.data->>'TemperatureAvg')::numeric, 0) AS temperature
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      LEFT JOIN hierarchy_level hl ON h.level_id = hl.id
      LEFT JOIN device_latest l ON l.serial_number= d.serial_number
      WHERE d.company_id = $1
    `;

    const queryParams = [company_id];
    let paramIndex = 2;

    // Add search filter
    if (search) {
      query += ` AND (d.serial_number ILIKE $${paramIndex} OR dt.type_name ILIKE $${paramIndex} OR h.name ILIKE $${paramIndex})`;
      queryParams.push(`%${search}%`);
      paramIndex++;
    }

    // Add status filter
    if (status !== 'all') {
      const statusCondition = status === 'online' 
        ? `l.updated_at >= now() - interval '5 minutes'`
        : `(l.updated_at IS NULL OR l.updated_at < now() - interval '5 minutes')`;
      
      query += ` AND ${statusCondition}`;
    }

    // Add device type filter
    if (deviceType !== 'all') {
      query += ` AND dt.type_name = $${paramIndex}`;
      queryParams.push(deviceType);
      paramIndex++;
    }

    // Add pagination
    query += ` ORDER BY dt.type_name, d.serial_number LIMIT $${paramIndex} OFFSET $${paramIndex + 1}`;
    queryParams.push(limit, offset);

    const result = await database.query(query, queryParams);

    // Get total count for pagination
    let countQuery = `
      SELECT COUNT(DISTINCT d.id) as total
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      LEFT JOIN device_latest l ON l.serial_number= d.serial_number
      WHERE d.company_id = $1
    `;

    const countParams = [company_id];
    let countParamIndex = 2;

    if (search) {
      countQuery += ` AND (d.serial_number ILIKE $${countParamIndex} OR dt.type_name ILIKE $${countParamIndex} OR COALESCE(h.name, '') ILIKE $${countParamIndex})`;
      countParams.push(`%${search}%`);
      countParamIndex++;
    }

    if (status !== 'all') {
      const statusCondition = status === 'online' 
        ? `l.updated_at >= now() - interval '5 minutes'`
        : `(l.updated_at IS NULL OR l.updated_at < now() - interval '5 minutes')`;
      
      countQuery += ` AND ${statusCondition}`;
    }

    if (deviceType !== 'all') {
      countQuery += ` AND dt.type_name = $${countParamIndex}`;
      countParams.push(deviceType);
    }

    const countResult = await database.query(countQuery, countParams);
    const totalDevices = parseInt(countResult.rows[0].total);

    // Get statistics
    const statsQuery = `
      SELECT 
        COUNT(DISTINCT d.id) as total_devices,
        COUNT(DISTINCT CASE WHEN l.updated_at >= now() - interval '5 minutes' THEN d.id END) as online_devices,
        (SELECT COUNT(*) FROM device_alarms da 
         JOIN device dev ON da.device_serial = dev.serial_number 
         WHERE dev.company_id = $1) as total_alarms
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      LEFT JOIN device_latest l ON l.serial_number= d.serial_number
      WHERE d.company_id = $1
    `;

    const statsResult = await database.query(statsQuery, [company_id]);
    const stats = statsResult.rows[0];

    // Get available device types
    const deviceTypesQuery = `
      SELECT DISTINCT dt.type_name
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      WHERE d.company_id = $1
      ORDER BY dt.type_name
    `;

    const deviceTypesResult = await database.query(deviceTypesQuery, [company_id]);

    res.json({
      success: true,
      message: 'Devices retrieved successfully',
      data: {
        devices: result.rows.map(row => ({
          deviceId: row.device_id,
          deviceSerial: row.device_serial,
          deviceName: row.device_name,
          deviceLogo: row.device_logo,
          wellName: row.hierarchy_name || 'Unassigned',
          hierarchyLevel: row.hierarchy_level,
          metadata: row.metadata,
          location: {
            longitude: row.longitude,
            latitude: row.latitude
          },
          lastCommTime: row.last_comm_time,
          status: row.status,
          flowData: {
            gfr: parseFloat(row.gfr) || 0,
            gor: parseFloat(row.gor) || 0,
            ofr: parseFloat(row.ofr) || 0,
            wfr: parseFloat(row.wfr) || 0,
            gvf: parseFloat(row.gvf) || 0,
            wlr: parseFloat(row.wlr) || 0,
            pressure: parseFloat(row.pressure) || 0,
            temperature: parseFloat(row.temperature) || 0
          }
        })),
        pagination: {
          page,
          limit,
          total: totalDevices,
          pages: Math.ceil(totalDevices / limit)
        },
        statistics: {
          totalDevices: parseInt(stats.total_devices) || 0,
          onlineDevices: parseInt(stats.online_devices) || 0,
          offlineDevices: (parseInt(stats.total_devices) || 0) - (parseInt(stats.online_devices) || 0),
          totalAlarms: parseInt(stats.total_alarms) || 0
        },
        filters: {
          availableDeviceTypes: deviceTypesResult.rows.map(row => row.type_name),
          currentFilters: {
            search,
            status,
            deviceType
          }
        }
      }
    });
  } catch (error) {
    console.error('Get all devices error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting devices'
    });
  }
});

// @desc    Get device by ID with detailed information
// @route   GET /api/devices/:id
// @access  Private
router.get('/:id', protect, async (req, res) => {
  try {
    const deviceId = parseInt(req.params.id);

    if (!deviceId || isNaN(deviceId)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid device ID provided'
      });
    }

    const query = `
      SELECT 
        d.id AS device_id,
        d.serial_number AS device_serial,
        dt.type_name AS device_name,
        dt.logo AS device_logo,
        d.metadata,
        d.created_at,
        h.name AS hierarchy_name,
        hl.name AS hierarchy_level,
        c.name AS company_name,
        l.longitude,
        l.latitude,
        l.updated_at AS last_comm_time,
        l.received_at,
        CASE 
          WHEN l.updated_at >= now() - interval '5 minutes' THEN 'Online'
          ELSE 'Offline'
        END AS status,
        l.data AS latest_data
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      JOIN company c ON d.company_id = c.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      LEFT JOIN hierarchy_level hl ON h.level_id = hl.id
      LEFT JOIN device_latest l ON l.serial_number = d.serial_number
      WHERE d.id = $1
    `;

    const result = await database.query(query, [deviceId]);

    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Device not found'
      });
    }

    const device = result.rows[0];

    // Check if user has access to this device
    if (req.user.role !== 'admin') {
      const accessCheck = await database.query(
        'SELECT company_id FROM device WHERE id = $1',
        [deviceId]
      );
      
      if (accessCheck.rows[0]?.company_id !== req.user.company_id) {
        return res.status(403).json({
          success: false,
          message: 'Access denied to this device'
        });
      }
    }

    // Get recent data history (last 24 hours)
    const historyQuery = `
      SELECT 
        created_at,
        data,
        longitude,
        latitude
      FROM device_data
      WHERE serial_number = $1 AND created_at >= now() - interval '24 hours'
      ORDER BY created_at DESC
      LIMIT 100
    `;

    const historyResult = await database.query(historyQuery, [device.device_serial]);

    res.json({
      success: true,
      message: 'Device retrieved successfully',
      data: {
        device: {
          deviceId: device.device_id,
          deviceSerial: device.device_serial,
          deviceName: device.device_name,
          deviceLogo: device.device_logo,
          wellName: device.hierarchy_name || 'Unassigned',
          hierarchyLevel: device.hierarchy_level,
          companyName: device.company_name,
          metadata: device.metadata,
          createdAt: device.created_at,
          location: {
            longitude: device.longitude,
            latitude: device.latitude
          },
          lastCommTime: device.last_comm_time,
          receivedAt: device.received_at,
          status: device.status,
          latestData: device.latest_data
        },
        recentHistory: historyResult.rows.map(row => ({
          timestamp: row.created_at,
          data: row.data,
          location: {
            longitude: row.longitude,
            latitude: row.latitude
          }
        }))
      }
    });
  } catch (error) {
    console.error('Get device by ID error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting device'
    });
  }
});

// @desc    Update device metadata
// @route   PUT /api/devices/:id
// @access  Private (Admin only)
router.put('/:id', protect, async (req, res) => {
  try {
    // Check if user is admin
    if (req.user.role !== 'admin') {
      return res.status(403).json({
        success: false,
        message: 'Admin access required'
      });
    }

    const deviceId = parseInt(req.params.id);
    const { metadata } = req.body;

    if (!deviceId || isNaN(deviceId)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid device ID provided'
      });
    }

    // Check if device exists
    const deviceCheck = await database.query('SELECT id FROM device WHERE id = $1', [deviceId]);
    
    if (deviceCheck.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Device not found'
      });
    }

    // Update device metadata
    const updateQuery = `
      UPDATE device 
      SET metadata = $2, updated_at = now()
      WHERE id = $1
      RETURNING *
    `;

    await database.query(updateQuery, [deviceId, JSON.stringify(metadata)]);

    res.json({
      success: true,
      message: 'Device updated successfully'
    });
  } catch (error) {
    console.error('Update device error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error updating device'
    });
  }
});

module.exports = router;