const express = require('express');
const Hierarchy = require('../models/Hierarchy');
const { protect } = require('../middleware/auth');

const router = express.Router();

// @desc    Get hierarchy tree for user's company or all companies (admin)
// @route   GET /api/hierarchy/tree
// @access  Private
router.get('/tree', protect, async (req, res) => {
  try {
    let company_id = null;
    
    // If user is not admin, restrict to their company
    if (req.user.role !== 'admin') {
      company_id = req.user.company_id;
    }
    
    // If admin requests specific company
    if (req.query.company_id && req.user.role === 'admin') {
      company_id = parseInt(req.query.company_id);
    }

    const hierarchyTree = await Hierarchy.getHierarchyTree(company_id);

    res.json({
      success: true,
      message: company_id ? 'Company hierarchy retrieved successfully' : 'All hierarchies retrieved successfully',
      data: {
        hierarchy: hierarchyTree,
        company_id: company_id,
        is_admin: req.user.role === 'admin',
        user_company_id: req.user.company_id
      }
    });
  } catch (error) {
    console.error('Get hierarchy tree error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting hierarchy tree'
    });
  }
});

// @desc    Get user's company hierarchy dashboard data
// @route   GET /api/hierarchy/dashboard
// @access  Private
router.get('/dashboard', protect, async (req, res) => {
  try {
    let company_id = req.user.company_id;
    
    // If admin requests specific company dashboard
    if (req.query.company_id && req.user.role === 'admin') {
      company_id = parseInt(req.query.company_id);
    }
    
    // Get hierarchy tree for user's company
    const hierarchyTree = await Hierarchy.getHierarchyTree(company_id);
    
    // Get company name
    const database = require('../config/database');
    const companyResult = await database.query('SELECT name FROM company WHERE id = $1', [company_id]);
    const companyName = companyResult.rows[0]?.name || 'Unknown Company';
    
    // Get detailed statistics
    const statsQuery = `
      SELECT 
  COUNT(DISTINCT h.id) AS total_locations,
  COUNT(DISTINCT CASE WHEN hl.name = 'Region' THEN h.id END) AS regions,
  COUNT(DISTINCT CASE WHEN hl.name = 'Area'   THEN h.id END) AS areas,
  COUNT(DISTINCT CASE WHEN hl.name = 'Field'  THEN h.id END) AS fields,
  COUNT(DISTINCT CASE WHEN hl.name = 'Well'   THEN h.id END) AS wells,
  COUNT(DISTINCT d.id) AS total_devices
FROM hierarchy h
JOIN hierarchy_level hl ON h.level_id = hl.id
LEFT JOIN device d ON d.hierarchy_id = h.id   -- ← fixed here
WHERE h.company_id = $1;

    `;
    
    const statsResult = await database.query(statsQuery, [company_id]);
    const stats = statsResult.rows[0];
    
    // Get device type statistics
    const deviceTypeStatsQuery = `
      SELECT 
        dt.type_name,
        COUNT(d.id) as count
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      WHERE d.company_id = $1
      GROUP BY dt.type_name
      ORDER BY count DESC
    `;
    
    const deviceTypeStatsResult = await database.query(deviceTypeStatsQuery, [company_id]);
    const deviceTypeStats = deviceTypeStatsResult.rows;

    res.json({
      success: true,
      message: 'Dashboard data retrieved successfully',
      data: {
        company: {
          id: company_id,
          name: companyName
        },
        hierarchy: hierarchyTree,
        statistics: {
          totalLocations: parseInt(stats.total_locations),
          regions: parseInt(stats.regions),
          areas: parseInt(stats.areas),
          fields: parseInt(stats.fields),
          wells: parseInt(stats.wells),
          totalDevices: parseInt(stats.total_devices)
        },
        deviceTypeStats: deviceTypeStats.map(stat => ({
          type: stat.type_name,
          count: parseInt(stat.count)
        })),
        userRole: req.user.role
      }
    });
  } catch (error) {
    console.error('Get dashboard data error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting dashboard data'
    });
  }
});

// @desc    Get all devices for user's company
// @route   GET /api/hierarchy/devices
// @access  Private
router.get('/devices', protect, async (req, res) => {
  try {
    let company_id = req.user.company_id;
    
    // If admin requests specific company devices
    if (req.query.company_id && req.user.role === 'admin') {
      company_id = parseInt(req.query.company_id);
    }

    const database = require('../config/database');
    const devicesQuery = `
      SELECT 
        d.id, d.serial_number, d.metadata, d.created_at,
        dt.type_name, dt.logo,
        h.name as location_name,
        c.name as company_name
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      JOIN company c ON d.company_id = c.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      WHERE d.company_id = $1
      ORDER BY dt.type_name, d.serial_number
    `;

    const result = await database.query(devicesQuery, [company_id]);
    const devices = result.rows.map(row => ({
      id: row.id,
      serial_number: row.serial_number,
      type: row.type_name,
      logo: row.logo,
      metadata: row.metadata || {},
      created_at: row.created_at,
      location: row.location_name,
      company: row.company_name
    }));

    res.json({
      success: true,
      message: 'Devices retrieved successfully',
      data: {
        devices,
        total: devices.length,
        company_id: company_id
      }
    });
  } catch (error) {
    console.error('Get devices error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting devices'
    });
  }
});

// @desc    Get device by ID
// @route   GET /api/hierarchy/devices/:id
// @access  Private
router.get('/devices/:id', protect, async (req, res) => {
  try {
    const deviceId = parseInt(req.params.id);
    const database = require('../config/database');
    
    const deviceQuery = `
      SELECT 
        d.id, d.serial_number, d.metadata, d.created_at,
        dt.type_name, dt.logo,
        h.name as location_name,
        c.id as company_id, c.name as company_name
      FROM device d
      JOIN device_type dt ON d.device_type_id = dt.id
      JOIN company c ON d.company_id = c.id
      LEFT JOIN hierarchy h ON d.hierarchy_id = h.id
      WHERE d.id = $1
    `;

    const result = await database.query(deviceQuery, [deviceId]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Device not found'
      });
    }

    const device = result.rows[0];
    
    // Check if user has access to this device
    if (req.user.role !== 'admin' && device.company_id !== req.user.company_id) {
      return res.status(403).json({
        success: false,
        message: 'Access denied to this device'
      });
    }

    res.json({
      success: true,
      message: 'Device retrieved successfully',
      data: {
        device: {
          id: device.id,
          serial_number: device.serial_number,
          type: device.type_name,
          logo: device.logo,
          metadata: device.metadata || {},
          created_at: device.created_at,
          location: device.location_name,
          company: {
            id: device.company_id,
            name: device.company_name
          }
        }
      }
    });
  } catch (error) {
    console.error('Get device error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting device'
    });
  }
});

// @desc    Get all hierarchy nodes for user's company
// @route   GET /api/hierarchy
// @access  Private
router.get('/', protect, async (req, res) => {
  try {
    let hierarchies;
    
    if (req.user.role === 'admin') {
      // Admin can see all hierarchies or filter by company
      if (req.query.company_id) {
        hierarchies = await Hierarchy.findByCompany(parseInt(req.query.company_id));
      } else {
        // Get all hierarchies for all companies
        const query = `
          SELECT h.*, hl.name as level_name, hl.level_order, c.name as company_name,
                 ph.name as parent_name
          FROM hierarchy h
          JOIN hierarchy_level hl ON h.level_id = hl.id
          JOIN company c ON h.company_id = c.id
          LEFT JOIN hierarchy ph ON h.parent_id = ph.id
          ORDER BY c.name, hl.level_order, h.name
        `;
        const result = await require('../config/database').query(query);
        hierarchies = result.rows.map(row => new Hierarchy(row));
      }
    } else {
      // Regular users see only their company's hierarchy
      hierarchies = await Hierarchy.findByCompany(req.user.company_id);
    }

    res.json({
      success: true,
      data: {
        hierarchies: hierarchies.map(h => h.toJSON())
      }
    });
  } catch (error) {
    console.error('Get hierarchies error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting hierarchies'
    });
  }
});

// @desc    Get hierarchy by ID
// @route   GET /api/hierarchy/:id
// @access  Private
router.get('/:id', protect, async (req, res) => {
  try {
    const hierarchyId = req.params.id;
    
    // Validate hierarchyId is a valid number
    if (!hierarchyId || isNaN(parseInt(hierarchyId))) {
      return res.status(400).json({
        success: false,
        message: 'Invalid hierarchy ID provided'
      });
    }

    const hierarchy = await Hierarchy.findById(parseInt(hierarchyId));

    if (!hierarchy) {
      return res.status(404).json({
        success: false,
        message: 'Hierarchy not found'
      });
    }

    // Check if user has access to this hierarchy
    if (req.user.role !== 'admin' && hierarchy.company_id !== req.user.company_id) {
      return res.status(403).json({
        success: false,
        message: 'Access denied to this hierarchy'
      });
    }

    res.json({
      success: true,
      data: {
        hierarchy: hierarchy.toJSON()
      }
    });
  } catch (error) {
    console.error('Get hierarchy error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting hierarchy'
    });
  }
});

module.exports = router;
