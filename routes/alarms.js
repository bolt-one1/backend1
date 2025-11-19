const express = require('express');
const { body, validationResult } = require('express-validator');
const DeviceAlarm = require('../models/DeviceAlarm');
const AlarmType = require('../models/AlarmType');
const AlarmStatus = require('../models/AlarmStatus');
const { protect, admin } = require('../middleware/auth');

const router = express.Router();

// @desc    Get all alarms for user's company or specific hierarchy
// @route   GET /api/alarms
// @access  Private
router.get('/', protect, async (req, res) => {
  try {
    let company_id = req.user.company_id;
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
    } = req.query;

    // If admin requests specific company alarms
    if (req.query.company_id && req.user.role === 'admin') {
      company_id = parseInt(req.query.company_id);
    }

    const filters = {
      hierarchy_id: hierarchy_id ? parseInt(hierarchy_id) : null,
      device_serial,
      alarm_type_id: alarm_type_id ? parseInt(alarm_type_id) : null,
      status_id: status_id ? parseInt(status_id) : null,
      severity,
      page: parseInt(page),
      limit: parseInt(limit),
      sort_by,
      sort_order: sort_order.toUpperCase()
    };

    console.log('Fetching alarms for company_id:', company_id);
    console.log('Filters:', filters);

    let result;
    if (hierarchy_id) {
      // Get alarms for specific hierarchy and its children
      result = await DeviceAlarm.findByHierarchy(parseInt(hierarchy_id), company_id, filters);
    } else {
      // Get all alarms for company
      result = await DeviceAlarm.findByCompany(company_id, filters);
    }

    console.log('Query result:', result);

    // Get alarm statistics
    const statistics = await DeviceAlarm.getAlarmStatistics(company_id, hierarchy_id ? parseInt(hierarchy_id) : null);

    console.log('Statistics:', statistics);

    res.json({
      success: true,
      message: 'Alarms retrieved successfully',
      data: {
        alarms: result.alarms.map(alarm => alarm.toJSON()),
        pagination: result.pagination,
        statistics: {
          total: parseInt(statistics.total_alarms) || 0,
          active: parseInt(statistics.active_alarms) || 0,
          acknowledged: parseInt(statistics.acknowledged_alarms) || 0,
          resolved: parseInt(statistics.resolved_alarms) || 0,
          by_severity: {
            critical: parseInt(statistics.critical_alarms) || 0,
            major: parseInt(statistics.major_alarms) || 0,
            minor: parseInt(statistics.minor_alarms) || 0,
            warning: parseInt(statistics.warning_alarms) || 0
          }
        },
        filters: {
          hierarchy_id,
          device_serial,
          alarm_type_id,
          status_id,
          severity,
          sort_by,
          sort_order
        }
      }
    });
  } catch (error) {
    console.error('Get alarms error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting alarms'
    });
  }
});

// @desc    Get alarm by ID
// @route   GET /api/alarms/:id
// @access  Private
router.get('/:id', protect, async (req, res) => {
  try {
    const alarmId = parseInt(req.params.id);
    
    if (!alarmId || isNaN(alarmId)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid alarm ID provided'
      });
    }

    const alarm = await DeviceAlarm.findById(alarmId);
    
    if (!alarm) {
      return res.status(404).json({
        success: false,
        message: 'Alarm not found'
      });
    }

    // Check if user has access to this alarm
    if (req.user.role !== 'admin' && alarm.company_id !== req.user.company_id) {
      return res.status(403).json({
        success: false,
        message: 'Access denied to this alarm'
      });
    }

    res.json({
      success: true,
      message: 'Alarm retrieved successfully',
      data: {
        alarm: alarm.toJSON()
      }
    });
  } catch (error) {
    console.error('Get alarm by ID error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting alarm'
    });
  }
});

// @desc    Create new alarm
// @route   POST /api/alarms
// @access  Private (Admin only)
router.post('/', protect, admin, [
  body('device_serial')
    .notEmpty()
    .withMessage('Device serial is required'),
  body('alarm_type_id')
    .isInt({ min: 1 })
    .withMessage('Valid alarm type ID is required'),
  body('message')
    .optional()
    .trim()
    .isLength({ max: 500 })
    .withMessage('Message must be less than 500 characters'),
  body('metadata')
    .optional()
    .isObject()
    .withMessage('Metadata must be an object')
], async (req, res) => {
  try {
    // Check for validation errors
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        message: 'Validation failed',
        errors: errors.array()
      });
    }

    const { device_serial, alarm_type_id, message, metadata } = req.body;

    // Verify alarm type exists
    const alarmType = await AlarmType.findById(alarm_type_id);
    if (!alarmType) {
      return res.status(400).json({
        success: false,
        message: 'Invalid alarm type ID'
      });
    }

    // Create alarm
    const alarm = await DeviceAlarm.create({
      device_serial,
      alarm_type_id,
      message,
      metadata
    });

    // Get full alarm details
    const fullAlarm = await DeviceAlarm.findById(alarm.id);

    res.status(201).json({
      success: true,
      message: 'Alarm created successfully',
      data: {
        alarm: fullAlarm.toJSON()
      }
    });
  } catch (error) {
    console.error('Create alarm error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error creating alarm'
    });
  }
});

// @desc    Update alarm status (acknowledge/resolve)
// @route   PUT /api/alarms/:id/status
// @access  Private
router.put('/:id/status', protect, [
  body('status_id')
    .isInt({ min: 1, max: 4 })
    .withMessage('Valid status ID is required (1-4)')
], async (req, res) => {
  try {
    // Check for validation errors
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({
        success: false,
        message: 'Validation failed',
        errors: errors.array()
      });
    }

    const alarmId = parseInt(req.params.id);
    const { status_id } = req.body;

    if (!alarmId || isNaN(alarmId)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid alarm ID provided'
      });
    }

    // Check if alarm exists and user has access
    const alarm = await DeviceAlarm.findById(alarmId);
    if (!alarm) {
      return res.status(404).json({
        success: false,
        message: 'Alarm not found'
      });
    }

    if (req.user.role !== 'admin' && alarm.company_id !== req.user.company_id) {
      return res.status(403).json({
        success: false,
        message: 'Access denied to this alarm'
      });
    }

    // Update alarm status
    const updatedAlarm = await DeviceAlarm.updateStatus(alarmId, status_id, req.user.id);
    
    if (!updatedAlarm) {
      return res.status(400).json({
        success: false,
        message: 'Failed to update alarm status'
      });
    }

    // Get full updated alarm details
    const fullAlarm = await DeviceAlarm.findById(alarmId);

    res.json({
      success: true,
      message: 'Alarm status updated successfully',
      data: {
        alarm: fullAlarm.toJSON()
      }
    });
  } catch (error) {
    console.error('Update alarm status error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error updating alarm status'
    });
  }
});

// @desc    Get alarm types
// @route   GET /api/alarms/types
// @access  Private
router.get('/types/all', protect, async (req, res) => {
  try {
    const alarmTypes = await AlarmType.findAll();

    res.json({
      success: true,
      message: 'Alarm types retrieved successfully',
      data: {
        alarm_types: alarmTypes.map(type => type.toJSON())
      }
    });
  } catch (error) {
    console.error('Get alarm types error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting alarm types'
    });
  }
});

// @desc    Get alarm statuses
// @route   GET /api/alarms/statuses
// @access  Private
router.get('/statuses/all', protect, async (req, res) => {
  try {
    const alarmStatuses = await AlarmStatus.findAll();

    res.json({
      success: true,
      message: 'Alarm statuses retrieved successfully',
      data: {
        alarm_statuses: alarmStatuses.map(status => status.toJSON())
      }
    });
  } catch (error) {
    console.error('Get alarm statuses error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting alarm statuses'
    });
  }
});

// @desc    Get alarm dashboard data
// @route   GET /api/alarms/dashboard
// @access  Private
router.get('/dashboard/stats', protect, async (req, res) => {
  try {
    let company_id = req.user.company_id;
    const { hierarchy_id } = req.query;

    // If admin requests specific company dashboard
    if (req.query.company_id && req.user.role === 'admin') {
      company_id = parseInt(req.query.company_id);
    }

    const statistics = await DeviceAlarm.getAlarmStatistics(
      company_id, 
      hierarchy_id ? parseInt(hierarchy_id) : null
    );

    // Get recent alarms (last 10)
    const recentAlarmsResult = await DeviceAlarm.findByCompany(company_id, {
      page: 1,
      limit: 10,
      sort_by: 'created_at',
      sort_order: 'DESC'
    });

    res.json({
      success: true,
      message: 'Alarm dashboard data retrieved successfully',
      data: {
        statistics: {
          total: parseInt(statistics.total_alarms) || 0,
          active: parseInt(statistics.active_alarms) || 0,
          acknowledged: parseInt(statistics.acknowledged_alarms) || 0,
          resolved: parseInt(statistics.resolved_alarms) || 0,
          by_severity: {
            critical: parseInt(statistics.critical_alarms) || 0,
            major: parseInt(statistics.major_alarms) || 0,
            minor: parseInt(statistics.minor_alarms) || 0,
            warning: parseInt(statistics.warning_alarms) || 0
          }
        },
        recent_alarms: recentAlarmsResult.alarms.map(alarm => alarm.toJSON()),
        company_id,
        hierarchy_id: hierarchy_id ? parseInt(hierarchy_id) : null
      }
    });
  } catch (error) {
    console.error('Get alarm dashboard error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting alarm dashboard data'
    });
  }
});

module.exports = router;