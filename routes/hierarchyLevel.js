const express = require('express');
const HierarchyLevel = require('../models/HierarchyLevel');
const { protect } = require('../middleware/auth');

const router = express.Router();

// @desc    Get all hierarchy levels
// @route   GET /api/hierarchy-level
// @access  Private
router.get('/', protect, async (req, res) => {
  try {
    const levels = await HierarchyLevel.findAll();

    res.json({
      success: true,
      data: {
        levels: levels.map(level => level.toJSON())
      }
    });
  } catch (error) {
    console.error('Get hierarchy levels error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting hierarchy levels'
    });
  }
});

// @desc    Get hierarchy level by ID
// @route   GET /api/hierarchy-level/:id
// @access  Private
router.get('/:id', protect, async (req, res) => {
  try {
    const level = await HierarchyLevel.findById(parseInt(req.params.id));

    if (!level) {
      return res.status(404).json({
        success: false,
        message: 'Hierarchy level not found'
      });
    }

    res.json({
      success: true,
      data: {
        level: level.toJSON()
      }
    });
  } catch (error) {
    console.error('Get hierarchy level error:', error);
    res.status(500).json({
      success: false,
      message: 'Server error getting hierarchy level'
    });
  }
});

module.exports = router;